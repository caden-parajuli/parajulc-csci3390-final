package final_project

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD
import scala.util.Random
import org.apache.spark.SparkContext
import _root_.final_project.final_project._
import org.apache.spark.graphx.EdgeContext
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.graphx.TripletFields
import org.apache.spark.rdd.RDD


object PivotClustering {

  /**
   * Performs the PIVOT algorithm on g.
   * This assumes vertices are labeled starting with 1, with no skipped numbers
   * Returns the clustering, and the checkpoint directory used by the clustering
   */
  def pivot(g: Graph[Long, Long], seed: Long): (VertexRDD[(Long, Long)], String) = {
    println("\nSeed: " + seed)
    val spark = createSparkSession(projectName)
    val sc = spark.sparkContext

    sc.setCheckpointDir(checkpointDir)

    // Remove self-edges
    val g_no_self = g.subgraph(triplet => triplet.srcId != triplet.dstId)

    // Permute vertices
    val timeBefore = System.nanoTime()
    val g_perm = permute(sc, g_no_self, seed)
    val timeAfterPerm = System.nanoTime()
    printf("Permutation completed in %.2f s.\n", nanoToSeconds(timeBefore, timeAfterPerm))

    // Main PIVOT loop
    var g_curr = g_perm
    var clusters = sc.emptyRDD[(Long, Long)]
    while (g_curr.numVertices > 0) {
      println(g_curr.numVertices + " vertices left")
      val iter = pivot_iteration(g_curr)

      g_curr = iter._1
      clusters = clusters.union(iter._2)

      clusters.localCheckpoint()
      // GraphX does not support localCheckpoint, there is a stale PR for it at https://github.com/apache/spark/pull/13379
      g_curr.checkpoint()
    }

    println("Joining clusters")
    // Join with the original vertex IDs
    val joinedClusters = g.vertices.innerJoin(clusters){ case (id, vval, cluster) => (id, cluster) }

    val timeAfterPivot = System.nanoTime()
    printf("PIVOT completed in %.2f s.\n", nanoToSeconds(timeBefore, timeAfterPivot))

    return (joinedClusters, sc.getCheckpointDir.getOrElse(""))
  }

  def pivot_iteration(g_curr: Graph[Long, Long]): (Graph[Long, Long], VertexRDD[Long]) = {
      // Determine pivots
      val pivots = g_curr.aggregateMessages[Boolean](
        // Sends true to the smaller vertex
        triplet => {
          val srcGreater = triplet.srcAttr > triplet.dstAttr
          triplet.sendToDst(srcGreater)
          triplet.sendToSrc(!srcGreater)
        },
        _&&_,
        new TripletFields(true, true, false)
      )

      // Join vertices with the pivots
      val g_pivots = g_curr.outerJoinVertices[Boolean, (Long, Boolean)](pivots) { (id, oldAttr, isPivotOpt) => 
        isPivotOpt match {
          case Some(isPivot) => (oldAttr, isPivot)
          // If it has no neighbors, it's a pivot
          case None => (oldAttr, true)
        }
      }.cache()

      // Send messages from pivots
      val pivotMessages = g_pivots.aggregateMessages[Long](
        triplet => {
          // We need to send to the pivot itself, so it gets included in the cluster
          if (triplet.srcAttr._2) {
            triplet.sendToDst(triplet.srcAttr._1)
            triplet.sendToSrc(triplet.srcAttr._1)
          }
          // Pivots cannot be adjacent
          else if (triplet.dstAttr._2) {
            triplet.sendToDst(triplet.dstAttr._1)
            triplet.sendToSrc(triplet.dstAttr._1)
          }
        },
        (a, b) => if (a <= b) a else b,
        new TripletFields(true, true, false)
      )

      // Cluster according to pivot messages. Unclustered vertices are assigned 0, everything else is assigned a cluster
      val g_clustered = g_pivots.outerJoinVertices(pivotMessages) { (id, oldAttr, clusterOpt) => 
        clusterOpt match {
          case Some(cluster) => cluster
          // If it's a pivot without a message, then it has no neighbors and should be its own cluster
          case None => if (oldAttr._2) oldAttr._1 else 0
        }
      }

      // Add these to the final RDD
      val just_clustered_vertices = g_clustered.vertices.filter(vert => vert._2 != 0)

      // Restrict to the unclustered vertices
      val new_g = g_curr.mask(
        g_clustered.subgraph(
          x => true,
          (v, d) => d == 0
        )
      ).cache()

      return (new_g, just_clustered_vertices)
  }

  def permute(sc: SparkContext, g: Graph[Long, Long], seed: Long): Graph[Long, Long] = {
    println("Beginning permutation...")
    val rand: Random = new Random(seed)
    val num_vertices = g.numVertices

    val vertices = g.vertices
    val permutation = sc.parallelize(
      rand.shuffle(1L to num_vertices toSeq),
      numSlices = vertices.getNumPartitions)


    val keyedVertices = vertices.zipWithIndex.map{ case (v, i) => i -> v }
    val keyedPerm = permutation.zipWithIndex.map{ case (v, i) => i -> v }
    val vertexPermutation = VertexRDD(
      new PairRDDFunctions(keyedVertices).join(keyedPerm).map({ case (i, v) => (v._1._1, v._2) })
    )

    return g.joinVertices(vertexPermutation)((id, label, perm) => perm)
  }
}
