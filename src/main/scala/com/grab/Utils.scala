package com.grab

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, lit}
import org.apache.spark.sql.types.{StringType, StructType}

object Utils {
  val FAIL_ON_DATA_LOSS = true
  val KAFKA_BROKERS_LIST = "localhost:9092"
  val TRIGGER_INTERVAL = 30 * 1000 // 30 seconds
  val WATERMARK_DELAY = "5 seconds"
//  val CKPT_DIR = "/Users/amitranjan/Documents/Grab-DE/checkpoint"
  val CKPT_DIR = "hdfs://localhost:9000/tmp"
//  val CKPT_DIR = "s3://careem-analytics/test_2"
  val START_OFFSET: String = "earliest"
  val PHOENIX_ZK_URL = "localhost:2181"

  def getDfFromKafka(spark: SparkSession)(kafkaTopic:String, schema:StructType) = {
    import spark.implicits._

    println(s"Creating df from topic ${kafkaTopic}")
    val dfFromKafka = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKERS_LIST)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", START_OFFSET)
      .option("failOnDataLoss", FAIL_ON_DATA_LOSS)
      .load()
      .select(
        $"timestamp",
        from_json(col("value").cast(StringType), schema).as("data"))

    dfFromKafka
  }
}
