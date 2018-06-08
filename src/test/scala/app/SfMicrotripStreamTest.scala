package app

import app.SfMicroTripStreaming.spark
import model.JsonDFSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.concurrent.duration.Duration

object SfMicrotripStreamTest extends App {

  val spark = SparkSession
    .builder()
    .appName("SfMicrotripStreamTest")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._
  import org.apache.spark.sql.functions._

  val keyValueDF = spark.readStream.text("src/test/resources/sf_kafka")
    .as[String]
    .map { line =>
      val Array(key, value) = line.split("\\|").map(_.trim())
      (key, value)
    }.toDF("key", "value")

  val keyvalue = keyValueDF.writeStream.format("console").option("header", "true").option("truncate", false).option("numRows", 10).start()

  //*****  sf_microtrip *********************************************************/
  val microtrip = keyValueDF.where($"key" === "sf-microtrip")
    .select(from_json($"value", JsonDFSchema.sf_microtrip).as("js")).select("js.*")
  println("#### sf-microtrip ######")
  microtrip.printSchema()
  microtrip.select("data.*","companyId","sensorId","msgType").writeStream.format("console").option("header", "true").option("truncate", false).option("numRows", 10).start()

  //*****  sf_event **************************************************************/
  val event = keyValueDF.where($"key" === "sf-event")
    .select(from_json($"value", JsonDFSchema.sf_event).as("js")).select("js.*")
  println("#### sf-event ######")
  event.printSchema()
  event.select("data.*","companyId","sensorId","msgType").writeStream.format("console").option("header", "true").option("truncate", false).option("numRows", 10).start()

  //*****  sf_trip **************************************************************/
  val trip = keyValueDF.where($"key" === "sf-trip")
    .select(from_json($"value", JsonDFSchema.sf_trip).as("js")).select("js.*")
  println("#### sf_trip ######")
  trip.printSchema()
  trip.select("data.*","latestTrip","companyId","sensorId","msgType").writeStream.format("console").option("header", "true").option("truncate", false).option("numRows", 10).start()

  // send kafka sf-score
/*
  keyValueDF.where($"key" === "sf_trip").withColumn("new_key", lit("sf-score")).select("new_key", "value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "server01:9092, server02:9092")
    .option("topic", "sf-score")
    .start()
*/

// HBase Sink
/*
  val catalog = "sf_microtrip"
  microtrip.writeStream.queryName("hbase writer").format("spark.sink.HBaseSinkProvider")
    //.option("checkpointLocation", "_checkpoint")
    .option("hbasecat", catalog)
    .outputMode(OutputMode.Update())
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()
*/

  keyvalue.awaitTermination()

}
