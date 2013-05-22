package com.stripe.simmer

import java.io._
import java.net._

class UDPInput(port : Int) extends Input {
  def run(simmer : Simmer) {
    System.err.println("Listening on UDP port " + port)

    val sock = new DatagramSocket(port)
    val buf = new Array[Byte](1024)
    while(true) {
      val packet = new DatagramPacket(buf, buf.length)
      sock.receive(packet)
      val str = new String(packet.getData, 0, packet.getLength)
      val columns = str.split("\t")
      if(columns.size > 1)
        simmer.update(columns(0), columns(1))
    }
  }
}