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

}

