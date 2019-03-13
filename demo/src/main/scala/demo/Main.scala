package demo

import akka.actor.{ActorSystem, Props}
import com.rabbitmq.client.Channel
import com.spingo.op_rabbit.Binding.{Concrete, ExchangeDefinition, QueueDefinition}
import com.spingo.op_rabbit.Exchange.ExchangeType
import com.spingo.op_rabbit._
import com.spingo.op_rabbit.properties.Header
import com.spingo.op_rabbit.properties.HeaderValue.{IntHeaderValue, StringHeaderValue}
import org.joda.time.DateTime
import play.api.libs.json._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

case class Data(id: Int, sent: Long) {
  override def toString: String = s"Data(id=$id, sent=${new DateTime(sent)}"
}

case object Delayed extends ExchangeType("x-delayed-message")

object Binding {
  def delayed(
               queue: QueueDefinition[Concrete],
               exchange:
               ExchangeDefinition[Concrete] with
                 Exchange[Delayed.type],
               routingKey: String): Binding = new Binding {

    override val exchangeName: String = exchange.exchangeName
    override val queueName: String = queue.queueName

    def declare(c: Channel): Unit = {
      exchange.declare(c)
      queue.declare(c)
      c.queueBind(queueName, exchangeName, routingKey, null);
    }
  }
}

object Main extends App {

  import PlayJsonSupport._

  implicit val actorSystem = ActorSystem("demo")
  implicit val dataFormat = Json.format[Data]

  val rabbitControl = actorSystem.actorOf(Props(new RabbitControl))
  implicit val recoveryStrategy = RecoveryStrategy.nack(false)

  import ExecutionContext.Implicits.global

  val demoQueue = Queue("demo", durable = false, autoDelete = true)

  val delayedExchange = Exchange.plugin(
    exchangeType = Delayed,
    name = "delayed-exchange-demo",
    arguments = Seq(
      Header("x-delayed-type", StringHeaderValue("direct"))
    ))


  val delayedToQueueRoute = "delayed.demo"
  val subscription = Subscription.run(rabbitControl) {
    import Directives._
    channel(qos = 3) {
      consume(Binding.delayed(demoQueue, delayedExchange, delayedToQueueRoute)) {
        body(as[Data]) { data =>
          println(s"received ${data} at ${new DateTime}")
          ack
        }
      }
    }
  }
  val delayedPublisher = Publisher.exchange(delayedExchange, delayedToQueueRoute)
  val delayHeader = Header("x-delay", IntHeaderValue(5.seconds.toMillis.toInt))
  (1 to 10) foreach { n =>
    rabbitControl ! Message(Data(n, System.currentTimeMillis()), delayedPublisher, Seq(delayHeader))
    Thread.sleep(1000)
  }

  println(s"All messages sent at ${new DateTime}")
}
