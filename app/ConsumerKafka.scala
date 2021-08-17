import java.time.Duration
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{IntegerDeserializer, StringDeserializer}

import scala.collection.JavaConverters._

object KafkaTestCosumer extends App {

  val consumerProps = new Properties()
  consumerProps.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  consumerProps.setProperty(GROUP_ID_CONFIG , "gp-id-1")
  consumerProps.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest")
  consumerProps.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[IntegerDeserializer].getName)
  consumerProps.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "true")
  consumerProps.setProperty(AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")


  val consumer = new KafkaConsumer[Int, String](consumerProps)

  consumer.subscribe(List("topic-with-key").asJava)


  while(true){
    val msg = consumer.poll(Duration.ofSeconds(3)).asScala

    consumer.commitAsync()

    for(i <- msg) {
      println(s"Topic: ${i.topic()},")
      println(s"Partition: ${i.partition()},")
      println(s"Offset: ${i.offset()},")
      println(s"Key: ${i.key()},")
      println(s"Value: ${i.value()}")
      println(s"Timestamp: ${i.timestamp()}")
    }
  }
}
