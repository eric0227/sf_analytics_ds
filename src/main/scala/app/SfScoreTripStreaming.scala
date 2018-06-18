package app

import java.util.UUID

import app.SfScoreMultiTripStreaming.{phoenixEnable, _}
import app.udf.SfScoreUDF
import model.{HBaseCatalog, JsonDFSchema}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.joda.time.LocalDate

import scala.concurrent.duration._
import app.udf.SfScoreUDF._
import com.datastax.spark.connector.cql.CassandraConnector
import common.TreanaConfig
import kafka.TreKafka
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.{count, from_json}
import org.apache.phoenix.spark._
import org.apache.spark.sql.functions._

object SfScoreTripStreaming {

  val bootstrap = "192.168.203.105:9092"
  val phoenixEnable = true

  val configuration = new Configuration()
  configuration.set("hbase.zookeeper.quorum", "server01:2181")

  def main(args: Array[String]) = {

    val spark = createSparkSession(args)

    // kafka topic(sf-score)
    val keyValueDF = kafkaKeyValueDF(spark, bootstrap)
    val kafkaQuery = printKafkaDFCount(keyValueDF)
    SfUtil.printConsole(keyValueDF)

    val tripDF = streamTripDF(keyValueDF)
    val microTripDF = SfUtil.loadMicroTripDF(spark.sqlContext, phoenix=phoenixEnable)
    val eventDF = SfUtil.loadEventDF(spark.sqlContext, phoenix=phoenixEnable)

    scoreProc(spark)

    kafkaQuery.awaitTermination()

  }

  def createSparkSession(args: Array[String]): SparkSession = {
    val master = if(args.length == 1) Some(args(0)) else None
    val bulder = SparkSession.builder()
      .appName("SfScoreTripStreaming")
      .config("spark.sql.streaming.checkpointLocation", "_checkpoint/SfScoreTripStreaming")
    master.foreach(mst => bulder.master(mst))
    val spark = bulder.getOrCreate()
    spark
  }

  def kafkaKeyValueDF(spark: SparkSession, bootstrap: String): DataFrame = {
    import spark.implicits._

    val keyValueDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", "sf-score")
      .option("startingOffsets", "latest") // // earliest, latest
      .load()
      .selectExpr("timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(Timestamp, String, String)].toDF("timestamp", "key", "value")
    keyValueDF
  }

  def printKafkaDFCount(kafkaDF: DataFrame): StreamingQuery = {
    kafkaDF.groupBy("key").agg(count("key"))
      .writeStream.format("console")
      .outputMode(OutputMode.Complete())
      .option("header", "true").start()
  }

  def streamTripDF(keyValueDF: DataFrame): DataFrame = {
    val spark = keyValueDF.sparkSession
    import spark.implicits._

    //*****  sf-score (Kafka) ********************************************************/
    val trip = keyValueDF.where($"key" === "sf-score")
      .select($"timestamp", from_json($"value", JsonDFSchema.sf_trip).as("value")).select("timestamp", "value.*")
    trip.createOrReplaceTempView("sf_trip")
    println("#### sf-trip (Kafka) ######")
    trip.printSchema()

    // debug
    SfUtil.printConsole(trip.select("data.*", "latestTrip", "msgType"), Some(3))

    trip.toDF()
  }


  def scoreProc(spark: SparkSession): StreamingQuery = {
    import spark.implicits._
    // join trip + microtrip + event
    val trip_microtrip_event = spark.sql(
      s"""
         | select t.timestamp           as timestamp
         |       ,t.data.tripId         as trip_id
         |       ,t.data.vehicleId      as vehicle_id
         |       ,t.data.userId         as user_id
         |       ,t.data.sensorId       as sensor_id
         |       ,t.data.companyId      as company_id
         |       ,t.data.startTs        as start_ts
         |       ,t.data.startDt        as start_dt
         |       ,t.data.endTs          as end_ts
         |       ,t.data.endDt          as end_dt
         |       ,t.data.deviceType     as device_type
         |       ,t.data.createdTime    as trip_created_time
         |       ,t.data.payload        as trip_payload_str
         |       ,t.data.updated        as updated
         |       ,t.latestTrip          as latest_trip
         |       ,t.msgType             as msg_type
         |       ,mt.created_time       as mtrip_created_time
         |       ,mt.date               as mtrip_date
         |       ,mt.payload            as mtrip_payload_str
         |       ,mt.ts                 as mtrip_ts
         |       ,e.payload             as event_payload_str
         | from            sf_trip t
         | inner join      sf_microtrip mt
         | on              t.data.tripId = mt.trip_id
         | left outer join sf_event e
         | on              t.data.tripId = e.trip_id
      """.stripMargin)
      .withColumn("trip_payload", from_json($"trip_payload_str", JsonDFSchema.trip_payload))
      .withColumn("mtrip_payload", explode(from_json($"mtrip_payload_str", JsonDFSchema.microtrip_payload)))

    trip_microtrip_event.createOrReplaceTempView("trip_microtrip_event")
    println("#### trip_microtrip_event ######")
    trip_microtrip_event.printSchema()
    // debug
    SfUtil.printConsole(trip_microtrip_event, Some(3))

    //********************** Trip 이동거리계산 ******************************************//
    val trip_dist_stat = trip_microtrip_event
      //.withWatermark(eventTime = "timestamp", delayThreshold = "10 seconds")
      .groupBy($"trip_id")
      .agg(
        count($"mtrip_payload").as("mtrip_cnt")
        , collect_list(struct($"mtrip_payload.clt", $"mtrip_ts", $"mtrip_payload.lon", $"mtrip_payload.lat", $"trip_id")).as("gps_list")
        , collect_list(struct($"mtrip_payload.sp", $"mtrip_payload.em", $"mtrip_payload.ldw", $"mtrip_payload.fcw")).as("trip_stat")
        , collect_set($"event_payload_str").as("event_payload")

        , first($"vehicle_id").as("vehicle_id")
        , first($"user_id").as("user_id")
        , first($"company_id").as("company_id")
        , first($"sensor_id").as("sensor_id")
        , first($"device_type").as("device_type")
        , first($"trip_payload.dis").as("trip_dist")
      )
      .withColumn("device_dist", SfScoreUDF.trip_distance( $"gps_list"))
      .withColumn("stat", SfScoreUDF.mtrip_and_event_stat($"trip_id", $"vehicle_id", $"user_id", $"company_id", $"device_type", $"device_dist", $"trip_stat", $"event_payload"))
      .withColumn("event_list", SfScoreUDF.event($"vehicle_id", $"user_id", $"gps_list", $"device_type"))
      .drop("gps_list", "trip_stat", "event_payload")

    println("#### trip_dist_stat ######")
    trip_dist_stat.printSchema()

    trip_dist_stat
      .writeStream.outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(3.seconds))
      .format("console").option("header", "true").option("truncate", false).start()
  }
}
