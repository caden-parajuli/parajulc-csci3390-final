package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object VerifyClustering {
  def lineToCanonicalEdge(line: String): Edge[Int] = {
    val x = line.split(",");

    if (x(0).toLong < x(1).toLong)
      Edge(x(0).toLong, x(1).toLong, 1)
    else
      Edge(x(1).toLong, x(0).toLong, 1)
  }

  def verifyClustering(g: Graph[Long, Long], clustering: VertexRDD[(Long, Long)]) {
    // Convert to plain RDD
    val clusters = clustering.values
    if (clusters.keys.distinct.count != clusters.count) {
      println("A vertex ID showed up more than once in the solution file.")
      sys.exit(1)
    }

    if (
      !(g.vertices.keys.subtract(clusters.keys).isEmpty() && clusters.keys
        .subtract(g.vertices.keys)
        .isEmpty())
    ) {
      println("The set of vertices in the solution file does not match that of the input file.")
    }
    
    println("The clustering has a disagreement of: " + numDisagreements(g, clustering))
  }

  def numDisagreements(g: Graph[Long, Long], clustering: VertexRDD[(Long, Long)]): Long = {
    val clusters = clustering.values
    val original_vertices = g.vertices.keys

    val clusters2 = clusters.map(x => (x._2, x._1))
    val clusters3 = clusters2
      .join(clusters.map(x => (x._2, 1L)).reduceByKey(_ + _))
      .map({ case (a, x) => (x._1, (a, x._2)) })

    val graph_vertices2 = clusters.map(x => (x._2, x._1))
    val graph_vertices3 = graph_vertices2
      .join(clusters.map(x => (x._2, 1L)).reduceByKey(_ + _))
      .map({ case (a, x) => (x._1, (a, x._2)) })

    val graph2: Graph[Tuple2[Long, Long], Long] =
      Graph(graph_vertices3, g.edges, (0L, 0L))

    val degrees = graph2.degrees
    val graph3 = graph2.outerJoinVertices(degrees) {
      case (id, (a, b), degOpt) =>
        degOpt match {
          case Some(deg) => (a, b, deg)
          case None      => (a, b, 0)
        }
    }
    val graph4 = graph3.aggregateMessages[Int](
      e => {
        if (e.srcAttr._1 == e.dstAttr._1) {
          e.sendToDst(1)
          e.sendToSrc(1)
        }
      },
      _ + _
    )

    val graph5 = graph3.outerJoinVertices(graph4) { (id, x, newOpt) =>
      newOpt match {
        case Some(newAttr) => (x._1, x._2, x._3, newAttr)
        case None          => (x._1, x._2, x._3, 0)
      }
    }
    val graph6 = graph5.mapVertices { (ID, ver) =>
      {
        val component_size = ver._2
        val degree = ver._3
        val same_component_agreement = ver._4
        (component_size - 1 - same_component_agreement) + (degree - same_component_agreement)
      }
    }

    val ans = graph6.vertices
      .aggregate(0L)({ case (x, v) => x + v._2 }, (x, y) => x + y)

    return ans / 2
  }
}
