name := "Grab"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.0"
val elasticsearchVersion = "7.2.0"
val phoenixVersion = "5.0.0-HBase-2.0"
val hbaseVersion = "2.0.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.elasticsearch" %% "elasticsearch-spark-20" % elasticsearchVersion,
  "org.apache.phoenix" % "phoenix-spark" % phoenixVersion,
  "org.apache.phoenix" % "phoenix-core" % phoenixVersion,
  "org.apache.hbase" % "hbase-common" % hbaseVersion,
  "org.apache.hbase" % "hbase-server" % hbaseVersion,
  "commons-httpclient" % "commons-httpclient" % "3.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "ch.hsr" % "geohash" % "1.3.0",
  "org.scalactic" %% "scalactic" % "3.0.1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
