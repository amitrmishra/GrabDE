name := "Grab"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.0"
val elasticsearchVersion = "7.2.0"
val phoenixVersion = "4.10.0-HBase-1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.elasticsearch" %% "elasticsearch-spark-20" % elasticsearchVersion,
  "org.apache.phoenix" % "phoenix-spark" % phoenixVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "ch.hsr" % "geohash" % "1.3.0",
  "org.scalactic" %% "scalactic" % "3.0.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
