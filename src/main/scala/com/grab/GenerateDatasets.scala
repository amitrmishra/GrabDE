package com.grab

import ch.hsr.geohash.GeoHash
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GenerateDatasets {
  def main(args: Array[String]): Unit = {
    val spark = getSparkSession
    import spark.implicits._

    //  val tripsCsvFile = args(0)
    val tripsCsvFile = "/Users/amitranjan/Documents/Grab-DE/green_tripdata_2016-01.csv"

    //  val tripsCsvFile = args(1)
    val weatherCsvFile = "/Users/amitranjan/Documents/Grab-DE/new_york_hourly_weather_data.csv"

    //  val outputDataDir = args(2)
    val outputDataDir = "/Users/amitranjan/Documents/Grab-DE/data"


    def getGeohash = udf((lat: Double, lon: Double) => GeoHash.geoHashStringWithCharacterPrecision(lat, lon, 6))
    def getHour = udf((timeString: String) => {
      val hour = timeString.split(":")(0).toInt
      if(timeString.split(" ")(1) == "AM") {
        if(hour == 12) hour - 12 else hour
      }
      else {
        if(hour == 12) hour else hour + 12
      }
    })

    val rawTripsDf = spark.read.format("csv").option("header", "true").load(tripsCsvFile)

    val tripsDf = rawTripsDf
      .select(to_timestamp($"lpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss").cast("long").as("pickup_epoch")
      ,to_timestamp($"Lpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss").cast("long").as("dropoff_epoch")
      ,$"Pickup_latitude".cast("double").as("pickup_latitude")
      ,$"Pickup_longitude".cast("double").as("pickup_longitude")
      ,$"Dropoff_latitude".cast("double").as("dropoff_latitude")
      ,$"Dropoff_longitude".cast("double").as("dropoff_longitude")
      ,$"Trip_distance".cast("float").as("trip_distance")
      ,$"Fare_amount".cast("float").as("fare_amounnt")
      ,$"Total_amount".cast("float").as("total_amount"))
      .withColumn("pickup_geohash", getGeohash($"pickup_latitude", $"pickup_longitude"))
      .withColumn("dropoff_geohash", getGeohash($"dropoff_latitude", $"dropoff_longitude"))
      .orderBy($"pickup_epoch")

    val bookingRequestDf = tripsDf
      .select('pickup_epoch.as("booking_time_epoch"),
        'pickup_geohash.as("booking_geohash"),
        'pickup_latitude.as("booking_latitude"),
        'pickup_longitude.as("booking_longitude"),
        rand().as("select_prob"))
      .union(tripsDf.select('dropoff_epoch.as("booking_time_epoch"),
        'pickup_geohash.as("booking_geohash"),
        'pickup_latitude.as("booking_latitude"),
        'pickup_longitude.as("booking_longitude"),
        rand().as("select_prob")))
      .union(tripsDf.select('pickup_epoch.as("booking_time_epoch"),
        'dropoff_geohash.as("booking_geohash"),
        'dropoff_latitude.as("booking_latitude"),
        'dropoff_longitude.as("booking_longitude"),
        rand().as("select_prob")))
      .union(tripsDf.select('dropoff_epoch.as("booking_time_epoch"),
        'dropoff_geohash.as("booking_geohash"),
        'dropoff_latitude.as("booking_latitude"),
        'dropoff_longitude.as("booking_longitude"),
        rand().as("select_prob")))
      .filter('select_prob > 0.6)
      .select(concat(lit("B"), ('select_prob * 1000000000).cast("long")).as("booking_id"),
        'booking_time_epoch,
        'booking_latitude,
        'booking_longitude,
        'booking_geohash)
      .orderBy('booking_time_epoch)

    val driverPingDf = tripsDf
      .select('pickup_epoch.as("driver_last_ping"),
        'pickup_geohash.as("driver_geohash"),
        'pickup_latitude.as("driver_latitude"),
        'pickup_longitude.as("driver_longitude"),
        rand().as("select_prob"))
      .union(tripsDf.select('dropoff_epoch.as("driver_last_ping"),
        'dropoff_geohash.as("driver_geohash"),
        'dropoff_latitude.as("driver_latitude"),
        'dropoff_longitude.as("driver_longitude"),
        rand().as("select_prob")))
      .union(tripsDf.select('pickup_epoch.as("driver_last_ping"),
        'dropoff_geohash.as("driver_geohash"),
        'dropoff_latitude.as("driver_latitude"),
        'dropoff_longitude.as("driver_longitude"),
        rand().as("select_prob")))
      .filter('select_prob > 0.5)
      .select(concat(lit("D"), ('select_prob * 1000000000).cast("long")).as("driver_id"),
        'driver_last_ping,
        'driver_geohash,
        'driver_latitude,
        'driver_longitude,
        when('select_prob > 0.5, 1).otherwise(0).as("driver_available"))
      .orderBy('driver_last_ping)

    // REALTIME
    // In the last 5 mins, number of bookings requested per geohash
    // Number of unique available driver pings per geohash
    // Get top 100 and bottom 100 such entries

    // BATCH
    // Same as above for the range (lower bound) and upper bound

//    val aggregatedBookingsDf = bookingRequestDf.groupBy('booking_geohash)
//      .agg(count("*").as("num_bookings"))
//
//    val availableDriversDf = driverPingDf.filter('driver_available === 1)
//      .groupBy('driver_geohash)
//      .agg(countDistinct('driver_id).as("num_active_drivers"))
//
//    val ratioDf = aggregatedBookingsDf
//      .join(availableDriversDf, 'booking_geohash === 'driver_geohash)
//        .where('booking_time_epoch.between('driver_last_ping, 'driver_last_ping.plus(5)))
//    ratioDf.select('booking_geohash, 'num_bookings, 'num_active_drivers, ('num_bookings/'num_active_drivers.cast("double")).as("ratio"))
//      .orderBy('ratio.desc)
//      .show(100)

    println(s"Trips: ${tripsDf.count}, Booking Requests: ${bookingRequestDf.count}, Drivers: ${driverPingDf.count}")


    val rawWeatherDf = spark.read.format("csv").option("header", "true").load(weatherCsvFile)
    val weatherDf = rawWeatherDf.select(to_timestamp(concat($"date", lit(" "), getHour($"TimeEST")), "yyyy-MM-dd HH").cast("long").as("start_epoch"),
        (to_timestamp(concat($"date", lit(" "), getHour($"TimeEST")), "yyyy-MM-dd HH").cast("long") + lit(3599)).as("end_epoch"),
        'TemperatureF.as("temperature"),
      $"Dew PointF".as("dew_point"),
      'Humidity.as("humidity"),
      $"Wind SpeedMPH".as("wind_speed"),
      'Conditions.as("condition"))
      .orderBy('start_epoch)

    tripsDf.repartition(1).write.json(s"$outputDataDir/trips")
    bookingRequestDf.repartition(1).write.json(s"$outputDataDir/booking-requests")
    driverPingDf.repartition(1).write.json(s"$outputDataDir/driver-ping")
    weatherDf.repartition(1).write.json(s"$outputDataDir/weather")
  }

  def getSparkSession(): SparkSession = {
    val sparkConf = new SparkConf()

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config(sparkConf)
      .getOrCreate()
    spark
  }
}
