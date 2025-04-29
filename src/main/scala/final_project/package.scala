package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

package object final_project {
  val projectName = "final_project"

  def createSparkSession(name: String): SparkSession = {
    SparkSession
      .builder()
      .appName(name)
      .getOrCreate()
  }

  def durationSeconds(startMillis: Long, endMillis: Long): Double = {
    return (endMillis - startMillis) / 1000
  }
}
