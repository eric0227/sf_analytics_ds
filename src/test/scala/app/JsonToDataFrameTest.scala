package app

import model.JsonDFSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object JsonToDataFrameTest extends App {

  val spark = SparkSession
    .builder()
    .appName("JsonToDataFrame")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  val gpsMicrotrip = spark.readStream
    .option("multiline", true).option("mode", "PERMISSIVE") // multiline 설정 안하면 라인단위로 json String 인식함
    .format("json").schema(JsonDFSchema.gps_microtrip).load("src/test/resources/json/gps_microtrip")

  gpsMicrotrip.createOrReplaceTempView("gps_microtrip")

  gpsMicrotrip.printSchema()

  gpsMicrotrip
    .writeStream.format("console")
    .option("truncate", false)
    .option("numRows", 10)
    .start()

  val query2 = spark.sql("select pld.tid, min(ts), max(ts), count(pld.nos) from gps_microtrip group by pld.tid")
    .writeStream.format("console")
    .option("truncate", false)
    .option("numRows", 10)
    .outputMode(OutputMode.Complete)
    .start()

  query2.awaitTermination()
}
