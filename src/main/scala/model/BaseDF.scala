package model

import java.util.UUID

import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.ClassTag

class BaseDF[T](ss : SparkSession, keyspace:String, sourceTable:String, partitionKeyName:String)
               (implicit ct:ClassTag[T], rrf: RowReaderFactory[T], ev: ValidRDDType[T]) extends StrictLogging {


  val sc = ss.sparkContext
  import ss.implicits._

  val udfMaxUUID = udf((data: Seq[String]) => {
    val list = data.map( UUID.fromString(_))
    list.reduce{ (a,b) => if ( compareUUID(a,b) > 0) a else b}.toString
  })

  def compareUUID( a:UUID, b:UUID) = a.timestamp() - b.timestamp()

  def nextPartitionKey( key: Option[String]) : Option[String] = {

    println(s"read table $keyspace $sourceTable ($partitionKeyName > $key)")

    val df = sc.cassandraTable[String](keyspace, sourceTable).select(partitionKeyName).toDF()
    val rec = (if ( key.isDefined) df.filter( df("value") > key.get) else df)
      .agg( min("value")).collect()

    if ( rec.size > 0) rec(0) match {
      case Row(r:String) => Some(r)
      case _ => None
    }
    else None
  }


  def list( keyValue:String, verbose:Boolean = false) : RDD[T] = {
    // read micro trip data
    val rdd = sc.cassandraTable[T](keyspace, sourceTable);
    logger.info( s"Read DB with '${partitionKeyName}=${keyValue}'")
    rdd.where(s"${partitionKeyName} = ?", keyValue)
  }

}
