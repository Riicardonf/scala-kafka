package configKafka

import java.util.Properties

import akka.actor.Status.Success
import org.apache.kafka.clients.producer.ProducerConfig._
import org.apache.kafka.clients.producer.{Callback, KafkaProducer,ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringSerializer}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json
import play.api.mvc.Results.Ok

import scala.concurrent.Future
import scala.util.Try

object ProducerKafka {

  val producerKafka = new Properties()

  producerKafka.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  producerKafka.setProperty(KEY_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getName)
  producerKafka.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerKafka.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true")
  producerKafka.setProperty(ACKS_CONFIG, "all")
 /* producerKafka.setProperty(TRANSACTIONAL_ID_CONFIG, "multas") */

  val producer =  new KafkaProducer[Int, String](producerKafka)


  def withAndWithOutKey(topic: String, key: Option[Int],  msg: String) =
    producer.send(prepareRecordToSend(topic, key, msg)
    )

  def withTransactions(topic: String, key: Option[Int],  msg: String) = {
    val p = new KafkaProducer[Int, String](producerKafka)
    p.initTransactions()
    Try{
      p.beginTransaction()
      p.send(prepareRecordToSend(topic, key, msg), new Callback() {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
          println(metadata.topic(), metadata.checksum(), metadata.offset(), metadata.timestamp())
      })
      p.commitTransaction()
      p.close()
    }
  }


  def prepareRecordToSend(topic: String, key: Option[Int],  msg: String): ProducerRecord[Int, String] = {
    key.fold(new ProducerRecord[Int, String](topic, msg)){
      key => new ProducerRecord[Int, String](topic, key, msg)
    }
  }

}
