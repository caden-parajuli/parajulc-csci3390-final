name := "final_project"

version := "1.0"

scalaVersion := "2.12.18"

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.4",
  "org.apache.spark" %% "spark-sql" % "3.5.4",
  // GraphX is found in lib folder
  "com.lihaoyi" %% "os-lib" % "0.11.3"
)

