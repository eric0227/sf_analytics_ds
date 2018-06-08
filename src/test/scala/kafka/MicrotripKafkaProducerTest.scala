package kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.FunSuite

class MicrotripKafkaProducerTest extends FunSuite  {

  case class MicroTripData(line: String) {
    val key =
      if (line.indexOf(""""msgType":"microtrip"""") > 0) "sf-microtrip"
      else if (line.indexOf(""""msgType":"trip"""") > 0) "sf-trip"
      else if (line.indexOf(""""msgType":"event"""") > 0) "sf-event"
      else "none"
  }

  val BOOTSTRAP_SERVERS = "192.168.203.105:9092"
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  test("sf-microtrip") {
    val source = scala.io.Source.fromURL(getClass.getResource("/sf_kafka/sf-microtrip.txt"))
    val list = source.getLines().map(line => MicroTripData(line)).filter(_.key == "sf-microtrip")
      //.take(5)
    list.foreach { data => sendKafka("sf-microtrip", data)}
  }

  test("sf-event") {
    val source = scala.io.Source.fromURL(getClass.getResource("/sf_kafka/sf-microtrip.txt"))
    val list = source.getLines().map(line => MicroTripData(line)).filter(_.key == "sf-event")
      //.take(5)
    list.foreach { data => sendKafka("sf-microtrip", data)}
  }

  test("sf-trip") {
    val source = scala.io.Source.fromURL(getClass.getResource("/sf_kafka/sf-microtrip.txt"))
    val list = source.getLines().map(line => MicroTripData(line)).filter(_.key == "sf-trip")
      .take(5)
    list.foreach { data => sendKafka("sf-microtrip", data)}
  }

  def sendKafka(topic: String, data: MicroTripData): Unit = {
    val record = new ProducerRecord[String, String](topic, data.key, data.line)
    val future = producer.send(record)
    val metadata = future.get()
    printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) \n", record.key, record.value, metadata.partition, metadata.offset)
  }
}
