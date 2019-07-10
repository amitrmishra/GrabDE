package com.grab

import ch.hsr.geohash.GeoHash
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GenerateDatasets {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession
    import spark.implicits._

    println("#######   supplied args     #######")
    args.foreach(println)
    println("###################################")

    val tripsCsvFile = if(args.length > 0) args(0) else "/Users/amitranjan/Documents/Grab-DE/yellow_tripdata_2016-01.csv"
    val weatherCsvFile = if(args.length > 1) args(1) else "/Users/amitranjan/Documents/Grab-DE/new_york_hourly_weather_data.csv"
    val outputDataDir = if(args.length > 2) args(2) else "/Users/amitranjan/Documents/Grab-DE/data"

    def getGeohash = udf((lat: Double, lon: Double) => GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 6))

    def getHour = udf((timeString: String) => {
      val hour = timeString.split(":")(0).toInt
      if(timeString.split(" ")(1).toUpperCase() == "AM") {
        if(hour == 12) hour - 12 else hour
      }
      else {
        if(hour == 12) hour else hour + 12
      }
    })

    val rawTripsDf = spark.read.format("csv").option("header", "true").load(tripsCsvFile)

    val tripsDf = rawTripsDf
      .select(to_timestamp($"tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss").cast("long").multiply(1000).as("pickup_epoch")
      ,to_timestamp($"tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss").cast("long").multiply(1000).as("dropoff_epoch")
      ,$"pickup_latitude".cast("double")
      ,$"pickup_longitude".cast("double")
      ,$"dropoff_latitude".cast("double")
      ,$"dropoff_longitude".cast("double")
      ,$"trip_distance".cast("float")
      ,$"fare_amount".cast("float")
      ,$"total_amount".cast("float"))
      .orderBy($"pickup_epoch")

    val bookingRequestDf = tripsDf
      .select('pickup_epoch.as("booking_time_epoch"),
        'pickup_latitude.as("booking_latitude"),
        'pickup_longitude.as("booking_longitude"),
        lit(getGeohash('pickup_latitude, 'pickup_longitude)).as("booking_geohash"),
        rand().as("select_prob"))
      .union(tripsDf.select('dropoff_epoch.as("booking_time_epoch"),
        'pickup_latitude.as("booking_latitude"),
        'pickup_longitude.as("booking_longitude"),
        lit(getGeohash('pickup_latitude, 'pickup_longitude)).as("booking_geohash"),
        rand().as("select_prob")))
      .union(tripsDf.select('pickup_epoch.as("booking_time_epoch"),
        'dropoff_latitude.as("booking_latitude"),
        'dropoff_longitude.as("booking_longitude"),
        lit(getGeohash('dropoff_latitude, 'dropoff_longitude)).as("booking_geohash"),
        rand().as("select_prob")))
      .union(tripsDf.select('dropoff_epoch.as("booking_time_epoch"),
        'dropoff_latitude.as("booking_latitude"),
        'dropoff_longitude.as("booking_longitude"),
        lit(getGeohash('dropoff_latitude, 'dropoff_longitude)).as("booking_geohash"),
        rand().as("select_prob")))
      .filter('select_prob > 0.6)
      .select(concat(lit("B"), ('select_prob * 1000000000).cast("long")).as("booking_id"),
        'booking_time_epoch,
        'booking_latitude,
        'booking_longitude,
        'booking_geohash
      )
      .orderBy('booking_time_epoch)

    val driverPingDf = tripsDf
      .select('pickup_epoch.as("driver_last_ping"),
        'pickup_latitude.as("driver_latitude"),
        'pickup_longitude.as("driver_longitude"),
        lit(getGeohash('pickup_latitude, 'pickup_longitude)).as("driver_geohash"),
        rand().as("select_prob"))
      .union(tripsDf.select('dropoff_epoch.as("driver_last_ping"),
        'dropoff_latitude.as("driver_latitude"),
        'dropoff_longitude.as("driver_longitude"),
        lit(getGeohash('dropoff_latitude, 'dropoff_longitude)).as("driver_geohash"),
        rand().as("select_prob")))
      .union(tripsDf.select('pickup_epoch.as("driver_last_ping"),
        'dropoff_latitude.as("driver_latitude"),
        'dropoff_longitude.as("driver_longitude"),
        lit(getGeohash('dropoff_latitude, 'dropoff_longitude)).as("driver_geohash"),
        rand().as("select_prob")))
      .filter('select_prob > 0.5)
      .select(concat(lit("D"), ('select_prob * 1000000000).cast("long")).as("driver_id"),
        'driver_last_ping,
        'driver_latitude,
        'driver_longitude,
        'driver_geohash,
        when('select_prob > 0.5, 1).otherwise(0).as("driver_available"))
      .orderBy('driver_last_ping)

    tripsDf.cache()

    println(s"Trips: ${tripsDf.count}, Booking Requests: ${bookingRequestDf.count}, Drivers: ${driverPingDf.count}")

    val rawWeatherDf = spark.read.format("csv").option("header", "true").load(weatherCsvFile)
    val weatherDf = rawWeatherDf.select(to_timestamp(concat($"date", lit(" "), getHour($"TimeEST")), "yyyy-MM-dd HH").cast("long").multiply(1000).as("start_epoch"),
        (to_timestamp(concat($"date", lit(" "), getHour($"TimeEST")), "yyyy-MM-dd HH").cast("long").multiply(1000) + lit(3599999)).as("end_epoch"),
        'TemperatureF.cast("float").as("temperature"),
      $"Dew PointF".cast("float").as("dew_point"),
      'Humidity.cast("float").as("humidity"),
      $"Wind SpeedMPH".cast("float").as("wind_speed"),
      'Conditions.as("condition"))
      .orderBy('start_epoch)

    tripsDf.repartition(1).write.mode("overwrite").json(s"$outputDataDir/trips")
    bookingRequestDf.repartition(1).write.mode("overwrite").json(s"$outputDataDir/booking-requests")
    driverPingDf.repartition(1).write.mode("overwrite").json(s"$outputDataDir/driver-ping")

    tripsDf.unpersist()

    weatherDf.repartition(1).write.mode("overwrite").json(s"$outputDataDir/weather")
  }

  def getSparkSession(): SparkSession = {
    val sparkConf = new SparkConf()

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config(sparkConf)
      .getOrCreate()

    // Avoid generating compressed output, as it may be used outside spark
    spark.sparkContext.hadoopConfiguration.set("mapred.output.compress", "false")
    spark
  } 
}
