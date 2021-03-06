package com.stripe.simmer

import org.rogach.scallop._

object Main {
	def main(args : Array[String]) {
		AlgebirdAggregators.load

		object Conf extends ScallopConf(args) {
			val version = "0.0.1"
			val capacity = opt[Int]("capacity", 'c', "maximum number of keys to keep in memory", Some(5000))
			val flushEvery = opt[Int]("flush", 'f', "flush a key once it hits this many values")
			val udp = opt[Int]("udp", 'u', "UDP port to listen for input")
			val redis = opt[String]("redis", 'r', "connect to Redis at host:port")
			val http = opt[Int]("http", 'h', "TCP port to listen for HTTP queries")
		}

		val input = Conf.udp.get match {
			case Some(port) => new UDPInput(port)
			case None => StdInput
		}

		val redis = Conf.redis.get.map{host => new Redis(host)}
		val output = redis.getOrElse(StdOutput)

		val simmer = new Simmer(output, Conf.capacity(), Conf.flushEvery.get)

		for(port <- Conf.http) {
			val lookup = redis.getOrElse(NullLookup)
			new Http(port, simmer, lookup)
		}

		input.run(simmer)
	}
}
