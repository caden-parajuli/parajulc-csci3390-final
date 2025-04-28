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


object PivotClustering {

  val checkpointDir = System.getProperty("user.dir") + "/checkpoints"

  /**
   * Performs the PIVOT algorithm on g.
   * This assumes vertices are labeled starting with 1, with no skipped numbers
   */
  def pivot(g: Graph[Long, Long], seed: Long): VertexRDD[(Long, Long)] = {
    println("\nSeed: " + seed)
    val spark = createSparkSession(projectName)
    val sc = spark.sparkContext

    sc.setCheckpointDir(checkpointDir)

    // Remove self-edges
    val g_no_self = g.subgraph(triplet => triplet.srcId != triplet.dstId)

    // Permute vertices
    val timeBefore = System.currentTimeMillis()
    val g_perm = permute(sc, g_no_self, seed)
    val timeAfterPerm = System.currentTimeMillis()
    println("Permutation completed in " + durationSeconds(timeBefore, timeAfterPerm) + " s.")

    // Main PIVOT loop
    var clustered_vertices = g_no_self.vertices
    var g_curr = g_perm
    var roundNum = 1
    while (g_curr.numVertices > 0) {
      println(g_curr.numVertices + " vertices left")

      // Determine pivots
      val pivots = g_curr.aggregateMessages[Boolean](
        // Sends true to the smaller vertex
        triplet => {
          val srcGreater = triplet.srcAttr > triplet.dstAttr
          triplet.sendToDst(srcGreater)
          triplet.sendToSrc(!srcGreater)
        },
        _&&_
      )

      // Join vertices with the pivots
      val g_pivots = g_curr.outerJoinVertices[Boolean, (Long, Boolean)](pivots) { (id, oldAttr, isPivotOpt) => 
        isPivotOpt match {
          case Some(isPivot) => (oldAttr, isPivot)
          // If it has no neighbors, it's a pivot
          case None => (oldAttr, true)
        }
      } //.cache()

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
        (a, b) => if (a <= b) a else b
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
      clustered_vertices = clustered_vertices.leftJoin(just_clustered_vertices) { (id, vertex, clusterOpt) => 
        clusterOpt match {
          // Leave vertices unchanged if we didn't get data for them
          case None => vertex
          // TODO think about if we really need to check for 0
          case Some(cluster) => if (cluster != 0) cluster else vertex
        }
      }

      // Restrict to the unclustered vertices
      g_curr = g_curr.mask(
        g_clustered.subgraph(
          x => true,
          (v, d) => d == 0
        )
      ).cache()

      // clustered_vertices does not have g_curr as an ancestor so we can checkpoint it first
      clustered_vertices = clustered_vertices.localCheckpoint()
      // GraphX does not support localCheckpoint, there is a stale PR for it at https://github.com/apache/spark/pull/13379
      g_curr.checkpoint()
    }

    // Join with the original vertex IDs
    val joinedClusters = g.vertices.innerZipJoin(clustered_vertices){ case (id, vval, cluster) => (id, cluster) }

    val timeAfterPivot = System.currentTimeMillis()
    println("PIVOT completed in " + durationSeconds(timeBefore, timeAfterPivot) + " s.")

    // Clear all checkpoints
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val status = fs.listStatus(new Path(checkpointDir))
    status.foreach(dir => {
        fs.delete(dir.getPath())
    })

    return joinedClusters
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
    // g.mapVertices((id, label: Long) => permutation(label))
  }
}
