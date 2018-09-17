name := "spark-sqs"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
  //"org.apache.hadoop" % "hadoop-aws" % "2.7.6",
  "org.apache.hadoop" % "hadoop-aws" % "3.0.0",
  "com.typesafe.play" %% "play-json" % "2.6.9" excludeAll ExclusionRule("com.fasterxml.jackson.core")
)