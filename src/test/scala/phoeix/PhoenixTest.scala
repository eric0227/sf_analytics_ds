package phoeix

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark._

object PhoenixTest {

  def createSparkSession() : SparkSession = {
    val bulder = SparkSession.builder()
      .appName("SfMicroTripStreaming")
      .master("local[4]")
      .config("spark.sql.streaming.checkpointLocation", "_checkpoint/PhoenixTest")
    val spark = bulder.getOrCreate()
    spark
  }

  def main(args: Array[String]): Unit = {

    val spark = createSparkSession()
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", "server01:2181")

    val df = spark.sqlContext.phoenixTableAsDataFrame(
        """"sf_microtrip""""
      , Array[String]()//Array("micro_trip_id", "vehicle_id", "trip_id", "payload", "ts")
      , conf = configuration
    )
    df.show(10)
  }
}
