package final_project

import org.apache.log4j.{Logger, Level}

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
        if (args.length < 2 || args.length > 3) {
          println("Usage: final_project pivot graph_path [seed]")
          sys.exit(1)
        }

        val seed = if (args.length == 3) {
          args(2).toLong
        } else {
          System.currentTimeMillis()
        }

        val g = FileIO.readInput(args(1))
        PivotClustering.pivot(g, seed)
      }
      case _ => {
        println("Usage: final_project option = {pivot, verifyPivot}")
        sys.exit(1)
      }
    }
  }
}
