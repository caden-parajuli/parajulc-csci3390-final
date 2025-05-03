package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

package object final_project {
  val projectName = "final_project"
  val checkpointDir = System.getProperty("user.dir") + "/checkpoints"
  def createSparkSession(name: String): SparkSession = {
    SparkSession
      .builder()
      .appName(name)
      .getOrCreate()
  }

  def durationSeconds(startMillis: Long, endMillis: Long): Double = {
    return (endMillis - startMillis).toDouble / 1000.0D
  }

  def nanoToSeconds(startNano: Long, endNano: Long): Double = {
    return (endNano - startNano).toDouble / 1000000000.0D
  }

  def printGraph(g: Graph[Long, Long]) {
    g.vertices.foreach({ case (id, v) => println("(" + id + ", " + v + ")")})
  }
}
