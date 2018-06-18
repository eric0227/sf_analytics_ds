package app

import app.udf.SfScoreUDF
import model.{HBaseCatalog, JsonDFSchema}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import scala.concurrent.duration._
import org.apache.spark.sql.functions._

object SfMicroTripStreaming {

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession(args)
    val bootstrap = "192.168.203.105:9092"

    // kafka topic(sf-microtrip)
    val keyValueDF = kafkaKeyValueDF(spark, bootstrap)
    val kafkaQuery = printKafkaDFCount(keyValueDF)

    // sf-microtrip 처리
    microtripProc(keyValueDF)

    // sf-event 처리
    eventProc(keyValueDF)

    // sf-trip 처리
    tripProc(keyValueDF, bootstrap)

    kafkaQuery.awaitTermination()
  }

  def createSparkSession(args: Array[String]): SparkSession = {
    val master = if(args.length == 1) Some(args(0)) else None
    val bulder = SparkSession.builder()
      .appName("SfMicroTripStreaming")
      .config("spark.sql.streaming.checkpointLocation", "_checkpoint/SfMicroTripStreaming")
    master.foreach(mst => bulder.master(mst))
    val spark = bulder.getOrCreate()
    spark
  }

  def kafkaKeyValueDF(spark: SparkSession, bootstrap: String): DataFrame = {
    import spark.implicits._

    val keyValueDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", "sf-microtrip")
      .option("startingOffsets", "latest") // // earliest, latest
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)].toDF("key", "value")
    keyValueDF
  }


  def printKafkaDFCount(kafkaDF: DataFrame): StreamingQuery = {
    kafkaDF.groupBy("key").agg(count("key"))
      .writeStream.format("console")
      .outputMode(OutputMode.Complete())
      .option("header", "true").start()
  }

  def microtripProc(keyValueDF: DataFrame): StreamingQuery = {
    val spark = keyValueDF.sparkSession
    import spark.implicits._

    //*****  sf_microtrip *********************************************************/
    val microtrip =
      keyValueDF.where($"key" === "sf-microtrip")
                .select(from_json($"value", JsonDFSchema.sf_microtrip).as("value")).select("value.*")
    microtrip.createOrReplaceTempView("sf_microtrip")
    println("#### sf-microtrip ######")
    microtrip.printSchema()

    SfUtil.printConsole(microtrip.select("data.*","msgType"), Some(3))

    // HBase  Sink -- microtrip
    spark.sql(
      s"""
         | select data.microTripId            as key1
         |       ,data.microTripId            as micro_trip_id
         |       ,data.vehicleId              as vehicle_id
         |       ,data.tripId                 as trip_id
         |       ,data.sensorId               as sensor_id
         |       ,data.createdTime            as created_time
         |       ,data.date                   as date
         |       ,data.deviceType             as device_type
         |       ,data.payload                as payload
         |       ,data.ts                     as ts
         |  from sf_microtrip
    """.stripMargin).na.fill("").writeStream.queryName("microtrip_sink")
      .format("spark.sink.HBaseSinkProvider")
      .option("hbasecat", HBaseCatalog.sf_microtrip)
      .outputMode(OutputMode.Append())
      .start()
  }

  def eventProc(keyValueDF: DataFrame): StreamingQuery = {
    val spark = keyValueDF.sparkSession
    import spark.implicits._

    //*****  sf_event **************************************************************/
    val event =
      keyValueDF.where($"key" === "sf-event")
                .select(from_json($"value", JsonDFSchema.sf_event).as("value")).select("value.*")
    event.createOrReplaceTempView("sf_event")
    println("#### sf-event ######")
    event.printSchema()

    SfUtil.printConsole(event.select("data.*", "msgType"), Some(3))

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
      """.stripMargin).na.fill("").writeStream.queryName("event_sink")
      .format("spark.sink.HBaseSinkProvider")
      .option("hbasecat", HBaseCatalog.sf_event)
      .outputMode(OutputMode.Append())
      .start()
  }


  def tripProc(keyValueDF: DataFrame, bootstrap: String): StreamingQuery = {
    val spark = keyValueDF.sparkSession
    import spark.implicits._

    //*****  sf_trip **************************************************************/
    val trip = keyValueDF.where($"key" === "sf-trip")
      .select(from_json($"value", JsonDFSchema.sf_trip).as("value")).select("value.*")
    trip.createOrReplaceTempView("sf_trip")
    println("#### sf_trip ######")
    trip.printSchema()

    SfUtil.printConsole(trip.select("data.*", "latestTrip", "msgType"), Some(3))

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
      .format("spark.sink.HBaseSinkProvider")
      .option("hbasecat", HBaseCatalog.sf_trip)
      .outputMode(OutputMode.Append())
      .start()

    // kafka Sink -- sf-score
    keyValueDF.where($"key" === "sf-trip").withColumn("key", lit("sf-score")).select("key", "value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("topic", "sf-score")
      .trigger(Trigger.ProcessingTime(5.seconds))
      .start()
  }
}
