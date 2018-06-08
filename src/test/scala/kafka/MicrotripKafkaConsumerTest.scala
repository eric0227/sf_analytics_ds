package kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.FunSuite

import scala.collection.JavaConverters._


class MicrotripKafkaConsumerTest extends FunSuite  {

  val TOPICS = "sf-microtrip,sf-score".split(",")
  val BOOTSTRAP_SERVERS = "192.168.203.105:9092"

  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // earliest, latest
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "sf-microtrip-group1")

  test("sf-microtrip,sf-score") {
    val consumer = new KafkaConsumer[String, String](props)
    println(props)

    consumer.subscribe(TOPICS.toList.asJava)
    try {
      while (true) {
        val records = consumer.poll(100)
        val list = records.asScala.toList
        println("record size : " + list.size)
        for (record <- list) {
          println(record)
        }
        Thread.sleep(5000)
      }
    } finally {
      consumer.close()
    }
  }

}
