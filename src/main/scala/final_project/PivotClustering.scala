package final_project

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexRDD
import scala.util.Random
import org.apache.spark.SparkContext
import _root_.final_project.final_project._
import org.apache.spark.graphx.EdgeContext

// TODO test with seed 1745304310713, figure out why it locks

object PivotClustering {
  /**
   * Performs the PIVOT algorithm on g.
   * This assumes vertices are labeled starting with 1, with no skipped numbers
   */
  def pivot(g: Graph[Long, Long], seed: Long) = {
    println("Seed: " + seed)
    val spark = createSparkSession(projectName)
    val sc = spark.sparkContext

    // We'll need this at the end, so cache it now
    g.cache()

    // Permute vertices
    val timeBeforePerm = System.currentTimeMillis()
    val g_perm = permute(sc, g, seed)
    val timeAfterPerm = System.currentTimeMillis()
    println("Permutation completed in " + durationSeconds(timeBeforePerm, timeAfterPerm) + " s.")

    // Main PIVOT loop
    var clustered_vertices = g.vertices
    var g_curr = g_perm //.cache()
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
      )

      // g_pivots.unpersistVertices()
    }

    spark.createDataFrame(clustered_vertices).show()
    // clustered_vertices.collect().foreach(println)
  }

  def permute(sc: SparkContext, g: Graph[Long, Long], seed: Long): Graph[Long, Long] = {
    print("Beginning permutation...")
    val rand: Random = new Random(seed)
    val num_vertices = g.numVertices

    // // Note that we include 0 since the first vertex label is 1, so we want to have the indices offset by 1
    // // This means a vertex will probably get a value of 0, but this does not affect the ordering
    // val permutation = rand.shuffle(0L to num_vertices toSeq)

    val vertices = g.vertices
    val permutation = sc.parallelize(
      rand.shuffle(1L to num_vertices toSeq),
      numSlices = vertices.getNumPartitions)
    // TODO: Alternative (test efficiency):
    // val permutation = sc.parallelize(0L to num_vertices)
    // permutation.sortBy((_) => rand.nextInt)

    val vertexPermutation = VertexRDD(
      vertices
        .zip(permutation)
        .map({case ((id, label), perm) => (id, perm) }))

    return g.joinVertices(vertexPermutation)((id, label, perm) => perm)
    // g.mapVertices((id, label: Long) => permutation(label))
  }
}
