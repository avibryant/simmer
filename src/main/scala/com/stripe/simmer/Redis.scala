package com.stripe.simmer

import com.twitter.finagle.redis.{TransactionalClient, ClientError}
import com.twitter.finagle.redis.util._
import com.twitter.finagle.redis.protocol.{Set => SetCommand}

class Redis(host : String) extends Output {
  System.err.println("Connecting to redis at " + host)
  val client = TransactionalClient(host)

  def write[A](key : String, value : A, aggregator : Aggregator[A]) : Boolean = {
    val keyCB = StringToChannelBuffer(":" + key)

    val future = client.watch(List(keyCB)).flatMap { unit =>
      client.get(keyCB).flatMap { result =>
        val newValue =
          result match {
            case Some(cb) => {
              val str = CBToString(cb)
              val oldValue = aggregator.deserialize(str).get
              aggregator.reduce(oldValue, value)
            }
            case None => value
          }

        val serialized = aggregator.serialize(newValue)
        val presented = aggregator.present(newValue)
        client.transaction(List(
          SetCommand(keyCB, StringToChannelBuffer(serialized)),
          SetCommand(StringToChannelBuffer(key), StringToChannelBuffer(presented))
        ))
      }
    }

    try {
      future.get
      true
    } catch {
      case ex : ClientError => {
        System.err.println(ex)
        false
      }
    }
  }
}