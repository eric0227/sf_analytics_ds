package app

import model.HBaseCatalog
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.phoenix.spark._

object SfUtil {
  // print Streaming DataFrame
  def printConsole(df: DataFrame, numRows: Option[Int] = None): Unit = {
    val writer = df.writeStream.format("console").option("header", "true").option("truncate", false)
    (numRows match {
      case Some(i) => writer.option("numRows", i)
      case _ => writer
    }).start()
  }

  def withCatalog(cat: String, sqlContext: SQLContext): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def loadMicroTripDF(sqlContext: SQLContext, phoenix: Boolean): DataFrame = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", "server01:2181")

    val df = phoenix match {
      case true =>
        sqlContext.phoenixTableAsDataFrame(
          """"sf_microtrip""""
          , Array[String]()//Array("micro_trip_id", "vehicle_id", "trip_id", "payload", "ts")
          , conf = configuration
        )
      case false => SfUtil.withCatalog(HBaseCatalog.sf_microtrip, sqlContext)
    }
    df.createOrReplaceTempView("sf_microtrip")
    println("#### sf_microtrip (HBase) ######")
    df.printSchema()
    df
  }

  def loadEventDF(sqlContext: SQLContext, phoenix: Boolean): DataFrame = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", "server01:2181")

    val df = phoenix match {
      case true => SfUtil.withCatalog(HBaseCatalog.sf_event, sqlContext)
      case false =>
        sqlContext.phoenixTableAsDataFrame(
          """"sf_event""""
          , Array[String]()//Array("micro_trip_id", "vehicle_id", "trip_id", "payload", "ts")
          , conf = configuration
        )
    }
    df.createOrReplaceTempView("sf_event")
    println("#### sf_event (HBase) ######")
    df.printSchema()
    df
  }

}
