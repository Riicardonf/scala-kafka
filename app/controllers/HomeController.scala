package controllers

import configKafka.ProducerKafka.{withAndWithOutKey, withTransactions}
import javax.inject._
import play.api._
import play.api.mvc._
import configKafka._
import org.apache.kafka.clients.producer.RecordMetadata
import play.api.data.Form
import play.api.data.Forms._
import play.api.libs.json.{Json, OFormat, Writes}

import scala.concurrent.{ExecutionContext, Future}

case class KafkaObjectForm(topic: String,  key: Option[Int], valueMsg: String)

@Singleton
class HomeController @Inject()
(val controllerComponents: ControllerComponents)(implicit ec: ExecutionContext) extends BaseController {

  val form: Form[KafkaObjectForm] = Form {
    mapping(
      "topic" -> nonEmptyText,
      "key" -> optional(number),
      "valueMsg" -> nonEmptyText
    )(KafkaObjectForm.apply)(KafkaObjectForm.unapply)
  }

  implicit val msgFormat: OFormat[KafkaObjectForm] = Json.format[KafkaObjectForm]

  def index() = Action.async(parse.json) {
    implicit request =>
      form.bindFromRequest().fold(
        errors => Future.successful(BadRequest),
        form => {

       val fromKafka = withAndWithOutKey(form.topic, form.key, form.valueMsg)

          Future.successful(Ok(s"💥 Topic: ${fromKafka.get().topic()}," +
            s"Partition: ${fromKafka.get().partition()}, " +
            s" Offset: ${fromKafka.get().offset()}, " +
            s"isDone: ${fromKafka.isDone}," +
            s" isCancelled: ${fromKafka.isCancelled}" +
            s" check: ${fromKafka.get().checksum()}"))
       //  withTransactions(form.topic, form.key, form.valueMsg)
         // Future.successful(Ok)
        }
      )

  }
}
