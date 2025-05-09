package final_project

import org.apache.log4j.{Logger, Level}
import org.apache.hadoop.fs.FileSystem
import _root_.final_project.final_project.createSparkSession
import org.apache.hadoop.fs.Path
import scala.util.Random
import _root_.final_project.FileIO.appendToFile
import _root_.final_project.FileIO.clearCheckpointsExcept
import _root_.final_project.FileIO.clearAllCheckpoints
import _root_.final_project.final_project.checkpointDir
import _root_.final_project.final_project.nanoToSeconds
import scala.collection.mutable.MutableList
import java.util.ArrayList
import _root_.final_project.VerifyClustering.verifyClustering

object main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  // Set Spark loggers to WARN
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  Logger.getLogger("final_project").setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("Usage: final_project option = {pivot, verifyClustering, findPivotSeeds}")
      sys.exit(1)
    }

    args(0) match {
      case "pivot" => {
        if (args.length < 3 || args.length > 4) {
          println("Usage: final_project pivot graph_path output_path [seed]")
          sys.exit(1)
        }

        clearAllCheckpoints(checkpointDir)
        
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
        val (clustering, _) = PivotClustering.pivot(g, seed)
        FileIO.writeClustering(clustering.values, outputPath)

        clearAllCheckpoints(checkpointDir)
      }

      case "findPivotSeeds" => {
        if (args.length < 3 || args.length > 4) {
          println("Usage: final_project findPivotSeeds graph_path output_path [num_trials]")
          sys.exit(1)
        }

        clearAllCheckpoints(checkpointDir)

        val inputPath = args(1)
        val outputPath = args(2)
        val numTrials = if (args.length == 4) {
          args(3).toLong
        } else {
          Long.MaxValue
        }

        val g = FileIO.readInput(inputPath)

        var seed = Random.nextInt(Int.MaxValue)
        var checkpoints: List[String] = Nil
        var trial = 0
        val timeBeforeStart = System.nanoTime()
        while (trial < numTrials) {
          val (clustering, lastCheckDir) = PivotClustering.pivot(g, seed)

          val timeBeforeDisagreements = System.nanoTime()
          val disagreements = VerifyClustering.numDisagreements(g, clustering.values)
          println("Disagreements: " + disagreements)
          val timeAfterDisagreements = System.nanoTime()
          printf("Disagreements computed in %.2f s.\n", nanoToSeconds(timeBeforeDisagreements, timeAfterDisagreements))
          printf("Total elapsed runtime: %.2f s.\n", nanoToSeconds(timeBeforeStart, timeAfterDisagreements))

          val output = seed + ":" + disagreements + "\n"
          appendToFile(output, outputPath)

          checkpoints = checkpointDir :: checkpoints
          if (checkpoints.length > 1) {
            clearCheckpointsExcept(checkpointDir, lastCheckDir)
            checkpoints = lastCheckDir :: Nil
          }

          // Note that we don't need the seeds to be random, just different from each other.
          // The pseudo-randomness inside pivot is enough as long as the seeds are distinct
          seed += 1
          trial += 1
        }

      }

      case "verifyClustering" => {
        if (args.length < 3 || args.length > 4) {
          println("Usage: final_project verifyClustering graph_path clustering_path")
          sys.exit(1)
        }
        val sc = createSparkSession("final_project").sparkContext

        val graphPath = args(1)
        val clusterPath = args(2)

        val g = FileIO.readInput(graphPath)
        val clusters = sc
          .textFile(clusterPath)
          .map(line => line.split(","))
          .map(parts => (parts(0).toLong, parts(1).toLong))

        verifyClustering(g, clusters)
      }

      case _ => {
        println("Usage: final_project option = {pivot, verifyClustering, findPivotSeeds}")
        sys.exit(1)
      }
    }
  }
}
