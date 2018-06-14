package kafka

import common.TreanaConfig
import common.TreanaConfig._
import model.{TreEventRow, TreMicroTripRow, TreTripRow}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object TreKafka {

  object kafkaParams {
    val producer = Map[String, Object](
      "bootstrap.servers" -> hosts,
      "key.serializer" -> classOf[StringSerializer].getName,
      "value.serializer" -> classOf[StringSerializer].getName,
      "group.id" -> "sf-producer-1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val consumer = Map[String, Object](
      "bootstrap.servers" -> hosts,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

  val hosts = TreanaConfig.config.getOrElse[String]("treana.kafka.servers", "localhost:9092")

  val interval = TreanaConfig.config.getOrElse[Int]("treana.kafka.interval", 5)

  val maxRate = TreanaConfig.config.getOrElse[String]("treana.kafka.maxRate", "50")

  val sparkCassandraHost = TreanaConfig.config.getString("treana.spark.cassandra.host")

  // topics
  val topicTrip = TreanaConfig.config.getOrElse[String]("treana.kafka.topics.trip", "sf-trip")

  val topicScore = TreanaConfig.config.getOrElse[String]("treana.kafka.topics.retry", "sf-score")

  val topicMicrotrip = TreanaConfig.config.getOrElse[String]("treana.kafka.topics.microtrip", "sf-microtrip")

  val topicEvent = TreanaConfig.config.getOrElse[String]("treana.kafka.topics.event", "sf-event")
}

case class DeviceTripId( deviceType:String, id:Option[String])

case class KafkaTrip( data:TreTripRow, latestTrip:Seq[DeviceTripId], companyId:String, sensorId:String, msgType:String)
case class KafkaTreEvent( data:TreEventRow, companyId:String, sensorId:String, msgType:String)
case class KafkaMicroTrip( data:TreMicroTripRow, companyId:String, sensorId:String, msgType:String)

case class AllKafkaMsg( mtrip:Option[TreMicroTripRow], trip:Option[TreTripRow], event:Option[TreEventRow], payload:Option[String])

