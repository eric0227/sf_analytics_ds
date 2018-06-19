package app

import model.HBaseCatalog
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.phoenix.spark._

object SfUtil {
  // print Streaming, DataFrame
  def printConsole(df: DataFrame, numRows: Option[Int] = None): Unit = {
    if (df.isStreaming) {
      val writer = df.writeStream.format("console").option("header", "true").option("truncate", false)
      (numRows match {
        case Some(i) => writer.option("numRows", i)
        case _ => writer
      }).start()
    } else {
      numRows match {
        case Some(i) => df.show(numRows = i, truncate = false)
        case _ => df.show(truncate = false)
      }
    }
  }

  def withCatalog(cat: String, sqlContext: SQLContext): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def loadTripDF(sqlContext: SQLContext, phoenix: Boolean, zookeeper: String): DataFrame = {
    val df = if(phoenix) {
      val configuration = new Configuration()
      configuration.set("hbase.zookeeper.quorum", zookeeper)

      sqlContext.phoenixTableAsDataFrame(
        """"sf_trip""""
        , Array[String]()//Array("micro_trip_id", "vehicle_id", "trip_id", "payload", "ts")
        , conf = configuration)
    } else {
      SfUtil.withCatalog(HBaseCatalog.sf_trip, sqlContext)
    }
    df.createOrReplaceTempView("sf_trip")
    println("#### sf_trip (HBase) ######")
    df.printSchema()
    df
  }

  def loadMicroTripDF(sqlContext: SQLContext, phoenix: Boolean, zookeeper: String): DataFrame = {
    val df = if(phoenix) {
      val configuration = new Configuration()
      configuration.set("hbase.zookeeper.quorum", zookeeper)

      sqlContext.phoenixTableAsDataFrame(
        """"sf_microtrip""""
        , Array[String]() //Array("micro_trip_id", "vehicle_id", "trip_id", "payload", "ts")
        , conf = configuration)
    } else {
      SfUtil.withCatalog(HBaseCatalog.sf_microtrip, sqlContext)
    }
    df.createOrReplaceTempView("sf_microtrip")
    println("#### sf_microtrip (HBase) ######")
    df.printSchema()
    df
  }

  def loadEventDF(sqlContext: SQLContext, phoenix: Boolean, zookeeper: String): DataFrame = {
    val df = if(phoenix) {
      val configuration = new Configuration()
      configuration.set("hbase.zookeeper.quorum", zookeeper)

      sqlContext.phoenixTableAsDataFrame(
        """"sf_event""""
        , Array[String]()//Array("micro_trip_id", "vehicle_id", "trip_id", "payload", "ts")
        , conf = configuration)
    } else {
      SfUtil.withCatalog(HBaseCatalog.sf_event, sqlContext)
    }
    df.createOrReplaceTempView("sf_event")
    println("#### sf_event (HBase) ######")
    df.printSchema()
    df
  }

}
