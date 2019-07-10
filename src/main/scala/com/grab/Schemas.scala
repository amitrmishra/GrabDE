package com.grab

import org.apache.spark.sql.types._

object Schemas {

  val SCHEMA_BOOKING_REQUESTS = StructType(
  List(StructField("booking_id", StringType, true),
    StructField("booking_time_epoch", LongType, true),
    StructField("booking_latitude", DoubleType, true),
    StructField("booking_longitude", DoubleType, true))
  )

  val SCHEMA_DRIVER_PING = StructType(
    List(StructField("driver_id", StringType, true),
      StructField("driver_last_ping", LongType, true),
      StructField("driver_available",LongType,true),
      StructField("driver_latitude", DoubleType, true),
      StructField("driver_longitude", DoubleType, true))
  )

  val SCHEMA_TRIPS = StructType(
    List(StructField("pickup_epoch", LongType, true),
      StructField("pickup_latitude", DoubleType, true),
      StructField("pickup_longitude", DoubleType, true),
      StructField("dropoff_epoch", LongType, true),
      StructField("dropoff_latitude", DoubleType, true),
      StructField("dropoff_longitude", DoubleType, true),
      StructField("trip_distance", DoubleType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("total_amount", DoubleType, true))
  )

  val SCHEMA_SURGE = StructType(
    List(StructField("BOOKING_GEOHASH", StringType, true),
      StructField("BOOKING_ID", StringType, true),
      StructField("DRIVER_ID", StringType, true))
  )

  val SCHEMA_WEATHER = StructType(
    List(
      StructField("start_epoch", LongType, true),
      StructField("end_epoch", LongType, true),
      StructField("condition", StringType, true),
      StructField("dew_point", DoubleType, true),
      StructField("humidity", DoubleType, true),
      StructField("temperature", DoubleType, true),
      StructField("wind_speed", DoubleType ,true)
    )
  )
}
