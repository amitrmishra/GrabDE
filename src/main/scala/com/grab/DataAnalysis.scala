package com.grab

import java.io.File
import java.util.Date

import ch.hsr.geohash.GeoHash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.elasticsearch.spark.sql._
import org.apache.phoenix.spark._
import org.apache.spark.sql.functions._

object DataAnalysis {
  def main(args: Array[String]): Unit = {

    def getGeohash = udf((lat: Double, lon: Double) => GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 6))

    val logger = Logger.getLogger(this.getClass.getName)
    logger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.es.nodes", "localhost")
    sparkConf.set("spark.es.port", "9200")
    sparkConf.set("spark.es.nodes.wan.only", "true")

    val spark = SparkSession
      .builder
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config(sparkConf)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    println("Creating tripsDf")
    val tripsDf = Utils.getDfFromKafka(spark)("trips", Schemas.SCHEMA_TRIPS)
      .withWatermark("timestamp", Utils.WATERMARK_DELAY)
      .filter($"data.dropoff_epoch".gt($"data.pickup_epoch"))
      .filter($"data.pickup_latitude".notEqual(0.0) || $"data.pickup_longitude".notEqual(0.0))
      .filter($"data.dropoff_latitude".notEqual(0.0) || $"data.dropoff_longitude".notEqual(0.0))
      .withColumn("dropoff_geohash", getGeohash($"data.dropoff_latitude", $"data.dropoff_longitude"))

//    println("Creating bookingRequestsDf")
//    val bookingRequestsDf = Utils.getDfFromKafka(spark)("booking_requests", Schemas.SCHEMA_BOOKING_REQUESTS)
//      .withColumn("booking_geohash", getGeohash($"data.booking_latitude", $"data.booking_longitude"))
//      .as("requests")
//      .withWatermark("timestamp", "5 seconds")
//
//    println("Creating driverPingsDf")
//    val driverPingsDf = Utils.getDfFromKafka(spark)("driver_pings", Schemas.SCHEMA_DRIVER_PING)
//      .withColumn("driver_geohash", getGeohash($"data.driver_latitude", $"data.driver_longitude"))
//      .as("pings")
//      .withWatermark("timestamp", "5 seconds")
//
//    val surgeDf = bookingRequestsDf.join(driverPingsDf,
//      expr("""
//         booking_geohash = driver_geohash AND
//         requests.timestamp >= pings.timestamp AND
//         requests.timestamp <= pings.timestamp + interval 5 seconds
//      """)
//    )
//    val surgeDf =

    println("Creating surgeDf")
    val surgeDf = Utils.getDfFromKafka(spark)("surge", Schemas.SCHEMA_SURGE)
        .select($"timestamp",
          $"data.BOOKING_GEOHASH".as("geohash"),
          $"data.BOOKING_ID".as("booking_id"),
          $"data.DRIVER_ID".as("driver_id")
        )

    println("Creating weatherDf")
    val weatherDf = Utils.getDfFromKafka(spark)("weather", Schemas.SCHEMA_WEATHER)

    println("Starting the streaming query...")
    val surgeRatioQuery: StreamingQuery = surgeDf
      .writeStream
      .trigger(Trigger.ProcessingTime(Utils.TRIGGER_INTERVAL))  // 30 seconds
      .option("checkpointLocation", s"${Utils.CKPT_DIR}/surge")
      .outputMode("append")
      .foreachBatch(new SurgeWriter())
      .start

    val congestionTimeQuery: StreamingQuery = tripsDf
      .writeStream
      .trigger(Trigger.ProcessingTime(Utils.TRIGGER_INTERVAL)) // 30 seconds
      .option("checkpointLocation", s"${Utils.CKPT_DIR}/trips")
      .outputMode("append")
      .foreachBatch(new CongestionWriter())
      .start

    val weatherQuery: StreamingQuery = weatherDf
      .writeStream
      .trigger(Trigger.ProcessingTime(Utils.TRIGGER_INTERVAL * 2 * 1)) // 1 minutes
      .option("checkpointLocation", s"${Utils.CKPT_DIR}/weather")
      .outputMode("append")
      .foreachBatch(new WeatherWriter())
      .start

    surgeRatioQuery.awaitTermination()
    congestionTimeQuery.awaitTermination()
    weatherQuery.awaitTermination()
  }
}

class CongestionWriter extends ((DataFrame, Long) => Unit) {
  def apply(data: DataFrame, batchId: Long): Unit = {
    import data.sparkSession.implicits._
    println(s"################  ${new Date}: [ CongestionWriter ] Started ################ ")
    val congestionData = data.groupBy($"timestamp".cast("long").plus(lit(5).minus($"timestamp".cast("long").mod(5))).multiply(1000).as("reporting_ts"), $"dropoff_geohash")
      .agg(sum($"data.trip_distance").as("total_distance"), sum($"data.dropoff_epoch".minus($"data.pickup_epoch")).divide(3600 * 1000).as("total_duration"))

    congestionData.saveToEs("grab_congestion")
    println("[ CongestionWriter ] Saved to elasticsearch")
    congestionData.saveToPhoenix(Map("table" -> "grab_congestion", "zkUrl" -> Utils.PHOENIX_ZK_URL))
    println("[ CongestionWriter ] Saved to phoenix")
  }
}

class SurgeWriter extends ((DataFrame, Long) => Unit) {
  def apply(data: DataFrame, batchId: Long): Unit = {
    data.sparkSession.sparkContext.setLogLevel("WARN")
    import data.sparkSession.implicits._
    println(s"################  ${new Date}: [ SurgeWriter ] Started ################ ")
    val surgeData = data.groupBy($"timestamp".cast("long").plus(lit(10).minus($"timestamp".cast("long").mod(10))).multiply(1000).as("reporting_ts"), $"geohash")
      .agg(countDistinct($"booking_id").as("num_requests"), countDistinct($"driver_id").as("num_drivers"))

    surgeData.saveToEs("grab_surge")
    println("[ SurgeWriter ] Saved to elasticsearch")
    surgeData.saveToPhoenix(Map("table" -> "grab_surge", "zkUrl" -> Utils.PHOENIX_ZK_URL))
    println("[ SurgeWriter ] Saved to phoenix")
  }
}

class WeatherWriter extends ((DataFrame, Long) => Unit) {
  def apply(data: DataFrame, batchId: Long): Unit = {
    data.sparkSession.sparkContext.setLogLevel("WARN")
    import data.sparkSession.implicits._
    println(s"################  ${new Date}: [ WeatherWriter ] Started ################ ")
    val weatherData = data.select(
      $"timestamp".cast("long").minus($"timestamp".cast("long").mod(3600)).multiply(1000).as("start_ts"),
      $"timestamp".cast("long").plus(lit(3600).minus($"timestamp".cast("long").mod(3600))).multiply(1000).as("end_ts"),
      $"data.condition".as("condition"),
      $"data.dew_point".as("dew_point"),
      $"data.humidity".as("humidity"),
      $"data.temperature".as("temperature"),
      $"data.wind_speed".as("wind_speed")
    )

    weatherData.saveToPhoenix(Map("table" -> "grab_weather", "zkUrl" -> Utils.PHOENIX_ZK_URL))
    println("[ WeatherWriter ] Saved to phoenix")
  }
}