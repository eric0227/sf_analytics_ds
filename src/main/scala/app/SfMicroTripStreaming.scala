package app

import app.SfMicroTripStreaming.spark
import model.{HBaseCatalog, JsonDFSchema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._

object SfMicroTripStreaming extends App {

  val master = if(args.length == 1) Some(args(0)) else None
  val bulder = SparkSession.builder().appName("SfMicroTripStreaming")
  master.foreach(mst => bulder.master(mst))
  val spark = bulder.getOrCreate()
  spark.conf.set("spark.sql.streaming.checkpointLocation", "_checkpoint/SfMicroTripStreaming")

  val bootstrap = "192.168.203.105:9092"

  val sc = spark.sparkContext
  import spark.implicits._
  import org.apache.spark.sql.functions._

  val keyValueDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", "sf-microtrip")
    .option("startingOffsets", "latest") // // earliest, latest
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)].toDF("key", "value")

  keyValueDF.groupBy("key").agg(count("key")).writeStream.format("console").outputMode(OutputMode.Complete()).option("header", "true")
    .trigger(Trigger.ProcessingTime(10.seconds)).start()
  //val keyvalue = keyValueDF.writeStream.format("console").option("header", "true").option("truncate", false).start()

  //*****  sf_microtrip *********************************************************/
  val microtrip = keyValueDF.where($"key" === "sf-microtrip")
    .select(from_json($"value", JsonDFSchema.sf_microtrip).as("value")).select("value.*")
  microtrip.createOrReplaceTempView("sf_microtrip")
  println("#### sf-microtrip ######")
  microtrip.printSchema()
  microtrip.select("data.*","msgType").writeStream.format("console").option("header", "true").option("truncate", false).option("numRows", 3).start()

  //*****  sf_event **************************************************************/
  val event = keyValueDF.where($"key" === "sf-event")
    .select(from_json($"value", JsonDFSchema.sf_event).as("value")).select("value.*")
  event.createOrReplaceTempView("sf_event")
  println("#### sf-event ######")
  event.printSchema()
  event.select("data.*","msgType").writeStream.format("console").option("header", "true").option("truncate", false).option("numRows", 3).start()

  //*****  sf_trip **************************************************************/
  val trip = keyValueDF.where($"key" === "sf-trip")
    .select(from_json($"value", JsonDFSchema.sf_trip).as("value")).select("value.*")
  trip.createOrReplaceTempView("sf_trip")
  println("#### sf_trip ######")
  trip.printSchema()
  trip.select("data.*","latestTrip","msgType").writeStream.format("console").option("header", "true").option("truncate", false).start()



// HBase  Sink -- microtrip
  val microtripWriteStream = spark.sql(
    s"""
      | select data.microTripId            as key1
      |       ,data.vehicleId              as vehicle_id
      |       ,data.tripId                 as trip_id
      |       ,data.sensorId               as sensor_id
      |       ,data.createdTime            as created_time
      |       ,data.date                   as date
      |       ,data.deviceType             as device_type
      |       ,data.payload                as payload
      |       ,data.ts                     as ts
      |  from sf_microtrip
    """.stripMargin).na.fill("").writeStream.queryName("microtrip hbase writer")
    .format("spark.sink.HBaseSinkProvider").option("checkpointLocation", "_checkpoint/microtrip").option("hbasecat", HBaseCatalog.sf_microtrip)
    .outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime(5.seconds)).start()



  // HBase  Sink -- trip
  val tripWriteStream = spark.sql(
    """
      | select data.tripId                  as key1
      |       ,data.vehicleId               as vehicle_id
      |       ,data.tripId                  as trip_id
      |       ,data.sensorId                as sensor_id
      |       ,data.createdTime             as created_time
      |       ,data.deviceType              as device_type
      |       ,data.endDt                   as end_dt
      |       ,data.endTs                   as end_ts
      |       ,data.startDt                 as start_dt
      |       ,data.startTs                 as start_ts
      |       ,data.payload                 as payload
      |       ,data.updated                 as updated
      |       ,data.userId                  as user_id
      |       ,'spark_index1'               as spark_idx
      |  from sf_trip
    """.stripMargin).na.fill("").writeStream.queryName("trip hbase writer")
    .format("spark.sink.HBaseSinkProvider").option("checkpointLocation", "_checkpoint/trip").option("hbasecat", HBaseCatalog.sf_trip)
    .outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime(5.seconds)).start()


  // HBase  Sink -- event
  spark.sql(
    """
      | select data.eventId                 as key1
      |       ,data.vehicleId               as vehicle_id
      |       ,data.tripId                  as trip_id
      |       ,data.eventId                 as event_id
      |       ,data.companyId               as company_id
      |       ,data.ty                      as ty
      |       ,data.sensorId                as sensor_id
      |       ,data.createdTime             as created_time
      |       ,data.eventDt                 as event_dt
      |       ,data.eventTs                 as event_ts
      |       ,data.payload                 as payload
      |       ,data.deviceType              as device_type
      |       ,data.userId                  as user_id
      |  from sf_event
    """.stripMargin).na.fill("").writeStream.queryName("event hbase writer")
    .format("spark.sink.HBaseSinkProvider").option("checkpointLocation", "_checkpoint/event").option("hbasecat", HBaseCatalog.sf_event)
    .outputMode(OutputMode.Append()).trigger(Trigger.ProcessingTime(5.seconds)).start()



  // kafka Sink -- sf-score
  keyValueDF.where($"key" === "sf-trip").withColumn("key", lit("sf-score")).select("key", "value")
    .writeStream
    .format("kafka")
    .option("checkpointLocation", "_checkpoint/sf-score")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("topic", "sf-score")
    .trigger(Trigger.ProcessingTime(5.seconds))
    .start()


    .awaitTermination()
}

