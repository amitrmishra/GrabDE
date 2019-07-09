package com.grab

import java.io.File
import java.util.Date

import ch.hsr.geohash.GeoHash
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._

object PushToEs {

//  Sample
  /**
    * // reusing the example from Spark SQL documentation
    *
    * import org.apache.spark.sql.SQLContext
    * import org.apache.spark.sql.SQLContext._
    *
    * import org.elasticsearch.spark.sql._
    *
    * ...
    *
    * // sc = existing SparkContext
    * val sqlContext = new SQLContext(sc)
    *
    * // case class used to define the DataFrame
    * case class Person(name: String, surname: String, age: Int)
    *
    * //  create DataFrame
    * val people = sc.textFile("people.txt")
    * .map(_.split(","))
    * .map(p => Person(p(0), p(1), p(2).trim.toInt))
    * .toDF()
    *
    * people.saveToEs("spark/people")
    */


  def getGeohash = udf((lat: Double, lon: Double) => GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 6))

  val logger = Logger.getLogger(this.getClass.getName)
  logger.setLevel(Level.ERROR)

  val FAIL_ON_DATA_LOSS = true

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sparkConf = new SparkConf()

  val kafkaBrokersList = "localhost:9092"
  val kafkaTopics = List("maxwell_binlog").mkString(",")
  val triggerInterval = 5000
  val checkpointDirectory = "/Users/amitranjan/Documents/Grab-DE/checkpoint"
  val startOffset: String = "earliest"

  println(s"Supplied configurations: kafkaBrokersList: $kafkaBrokersList," +
    s" kafkaTopicName: $kafkaTopics, triggerInterval: $triggerInterval, startOffset: $startOffset"
  )

  val spark = SparkSession
    .builder
    .appName(this.getClass.getSimpleName)
    .master("local[*]")
    .config(sparkConf)
    .getOrCreate()

  // Delete existing checkpoint directory
//  FileUtils.deleteDirectory(new File(checkpointDirectory));

  println("Creating rawStreamReader")
  val rawDf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBrokersList)
    .option("subscribe", "")
    .option("startingOffsets", startOffset)
    .option("failOnDataLoss", FAIL_ON_DATA_LOSS)
    .load()


  println("Starting the streaming query...")
  val query: StreamingQuery = rawDf
    .select(col("topic")
      ,col("timestamp")
      ,col("value").cast(StringType)
//      from_json(col("value").cast(StringType), PARQUET_WRITER_RAW_SCHEMA).as(PARQUET_WRITER_RAW_DATA_FIELD)
    )
    .writeStream
    .trigger(Trigger.ProcessingTime(triggerInterval))
    .option("checkpointLocation", checkpointDirectory)
    .outputMode("append")
    .foreachBatch(new TestParquetWriter())
    .start
  query.awaitTermination()
}

class TestParquetWriter extends ((DataFrame, Long) => Unit) {
  def apply(data: DataFrame, batchId: Long): Unit = {
    println(s"################  ${new Date}  ################ ")
    data.show(100, false)
    println(s"################    ################    ################ ")

  }
}
