name := "DdosDetector"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.5.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0" % "provided"