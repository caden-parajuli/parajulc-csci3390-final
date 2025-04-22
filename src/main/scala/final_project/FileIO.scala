package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import _root_.final_project.final_project.createSparkSession

object FileIO {
  val spark = createSparkSession("final_project")
  val sc = spark.sparkContext

  def lineToCanonicalEdge(line: String): Edge[Long] = {
    val x = line.split(",");

    if (x(0).toLong < x(1).toLong)
      Edge(x(0).toLong, x(1).toLong, 1)
    else
      Edge(x(1).toLong, x(0).toLong, 1)
  }

  def readInput(filename: String): Graph[Long, Long] = {
    val graph_edges = sc.textFile(filename).map(lineToCanonicalEdge)
    Graph.fromEdges[Long, Long](graph_edges, 0)
  }

  def writeClustering(clusterings: VertexRDD[(Long, Long)], filename: String) = {
    spark.createDataFrame(clusterings).coalesce(1).write.format("csv").mode(SaveMode.Overwrite).save(filename)
  }

}
