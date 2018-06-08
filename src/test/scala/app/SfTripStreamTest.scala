package app

import model.JsonDFSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object SfTripStreamTest extends App {

  val spark = SparkSession
    .builder()
    .appName("SfMicrotripStreamTest")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val gpsMicrotrip = spark.readStream
    .format("json").schema(JsonDFSchema.sf_trip).load("src/test/resources/json/sf_trip")

  gpsMicrotrip.createOrReplaceTempView("sf_trip")

  gpsMicrotrip.printSchema()

  gpsMicrotrip
    .writeStream.format("console")
    .option("truncate", false)
    .option("numRows", 10)
    .start()

  val query2 = spark.sql("select data.vehicleId, count(1) from sf_trip group by data.vehicleId")
    .writeStream.format("console")
    .option("truncate", false)
    .option("numRows", 10)
    .outputMode(OutputMode.Complete)
    .start()

  query2.awaitTermination()
}
