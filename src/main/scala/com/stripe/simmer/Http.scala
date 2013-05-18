package com.stripe.simmer

import com.twitter.finagle.Service
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.util.CharsetUtil.UTF_8
import com.twitter.util.Future
import java.net.InetSocketAddress
import com.twitter.finagle.builder.{Server, ServerBuilder}
import com.twitter.finagle.http.{Http => HttpCodec}

class Http(port : Int, simmer : Simmer, redis : Option[Redis]) {
  System.err.println("Listening on HTTP port " + port)

  ServerBuilder()
    .codec(HttpCodec())
    .bindTo(new InetSocketAddress(port))
    .name("http")
    .build(new Service[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest) = handle(request)
    })

  def extractKey(request : HttpRequest) : String = {
    val uri = request.getUri
    val parts = uri.split("[/?]")
    if(parts.size > 1)
      parts(1)
    else
      "sum:all"
  }

  def handle(request : HttpRequest) = {
    val key = extractKey(request)

    redis.get.read(key).map{result =>

      val response = new DefaultHttpResponse(HTTP_1_1, OK)
      val acc = simmer.accumulators.get(key)

      val output = result match {
        case Some((serialized, presented)) => {
          if(acc == null) {
            presented
          } else {
            acc.mergeAndPresent(serialized)
          }
        }

        case None => {
          if(acc == null) {
            ""
          } else {
            acc.present
          }
        }
      }

      response.setContent(copiedBuffer(output, UTF_8))
      response
    }
  }
}