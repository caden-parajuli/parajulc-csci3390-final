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
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object FileIO {
  private val spark = createSparkSession("final_project")
  private val sc = spark.sparkContext

  def lineToCanonicalEdge(line: String): Edge[Long] = {
    val x = line.split(",");

    if (x(0).toLong < x(1).toLong)
      Edge(x(0).toLong, x(1).toLong, 1)
    else
      Edge(x(1).toLong, x(0).toLong, 1)
  }

  def readInput(filename: String): Graph[Long, Long] = {
    val graph_edges = sc.textFile(filename).map(lineToCanonicalEdge)
    Graph.fromEdges[Long, Long](graph_edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
  }

  def writeClustering(clusterings: RDD[(Long, Long)], filename: String) = {
    println("Writing clustering to " + filename)
    clusterings.map({ case (vertex, cluster) => vertex + "," + cluster }).saveAsTextFile(filename)
    // val frame = spark.createDataFrame(clusterings)
    // frame.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).save(filename)
    // frame.show()
  }

  def appendToFile(str: String, path: String) = {
    val bw = new BufferedWriter(
      new FileWriter(new File(path), true)
    )
    bw.write(str)
    bw.close()
  }

  def clearAllCheckpoints(checkDir: String) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val status = fs.listStatus(new Path(checkDir))
    status.foreach(dir => {
        fs.delete(dir.getPath())
    })
  }

  /**
   * Clears all checkpoint folders except the specified one. Used to prevent Spark race conditions
   *
   * @param checkDir
   */
  def clearCheckpointsExcept(checkDir: String, keptDir: String) = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val status = fs.listStatus(new Path(checkDir))
    status.foreach(dir => {
        if (dir.getPath().getName() != new Path(keptDir).getName()) {
          fs.delete(dir.getPath())
        }
    })
  }

}
