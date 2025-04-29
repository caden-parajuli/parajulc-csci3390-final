package final_project

import org.apache.log4j.{Logger, Level}
import org.apache.hadoop.fs.FileSystem
import _root_.final_project.final_project.createSparkSession
import org.apache.hadoop.fs.Path
import scala.util.Random
import _root_.final_project.FileIO.appendToFile

object main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  // Set Spark loggers to WARN
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  Logger.getLogger("final_project").setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Usage: final_project option = {pivot, verifyPivot}")
      sys.exit(1)
    }

    args(0) match {
      case "pivot" => {
        if (args.length < 3 || args.length > 4) {
          println("Usage: final_project pivot graph_path output_path [seed]")
          sys.exit(1)
        }
        
        val inputPath = args(1)
        val outputPath = args(2)
        val seed = if (args.length == 4) {
          args(3).toLong
        } else {
          System.currentTimeMillis()
        }

        val sc = createSparkSession("final_project").sparkContext
        val fs = FileSystem.get(sc.hadoopConfiguration)
        if (fs.exists(new Path(outputPath))) {
          println("Output path already exists! Use a new path.")
          System.exit(1)
        }

        val g = FileIO.readInput(inputPath)
        val clustering = PivotClustering.pivot(g, seed)
        FileIO.writeClustering(clustering.values, outputPath)
      }

      case "findPivotSeeds" => {
        if (args.length < 3 || args.length > 4) {
          println("Usage: final_project findPivotSeeds graph_path output_path [num_trials]")
          sys.exit(1)
        }

        val inputPath = args(1)
        val outputPath = args(2)
        val numTrials = if (args.length == 4) {
          args(3).toLong
        } else {
          Long.MaxValue
        }

        val g = FileIO.readInput(inputPath)

        var seed = Random.nextInt(Int.MaxValue)
        var trial = 0
        while (trial < numTrials) {
          val clustering = PivotClustering.pivot(g, seed)
          val disagreements = VerifyClustering.numDisagreements(g, clustering)
          println("Disagreements: " + disagreements)

          val output = seed + ":" + disagreements + "\n"
          appendToFile(output, outputPath)

          // Note that we don't need the seeds to be random, just different from each other.
          // The pseudo-randomness inside pivot is enough as long as the seeds are distinct
          seed += 1
          trial += 1
        }

      }

      case _ => {
        println("Usage: final_project option = {pivot, verifyPivot, findPivotSeeds}")
        sys.exit(1)
      }
    }
  }
}
