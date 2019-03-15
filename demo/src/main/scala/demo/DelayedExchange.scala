package demo

import com.rabbitmq.client.Channel
import com.spingo.op_rabbit.Exchange
import com.spingo.op_rabbit.properties.Header
import com.spingo.op_rabbit.properties

case class DelayedExchange(
                            override val exchangeName: String,
                            durable: Boolean = true, autoDelete: Boolean = false, arguments: Seq[Header] = Seq()
                          ) extends Exchange[Nothing] {
  private val exchangeKind = "x-delayed-message"

  def declare(c: Channel): Unit = {
    c.exchangeDeclare(exchangeName, exchangeKind, durable, autoDelete, properties.toJavaMap(arguments))
  }
}
