package com.stripe.algescrubber

trait Aggregator[A] {
	def createTable = new Table(this)
	def prepare(input : String) : A
	def reduce(left : A, right : A) : A
	def serialize(value : A) : String
	def deserialize(serialized : String) : Option[A]
	def present(value : A) : String
}

object Registry {	
	var registry = Map[String,Option[Int]=>Aggregator[_]]()

	def register(typeKey : String, default : Int)(fn : Int=>Aggregator[_]) {
		registry += typeKey -> {(n:Option[Int]) => fn(n.getOrElse(default))}
	}

	def get(aggKey: String) = {
		val keyRegex = """([a-zA-Z]+)(\d*)""".r

		keyRegex.findFirstMatchIn(aggKey).flatMap{m =>
			registry.get(m.group(1)).map {fn =>
				val n = m.group(2)
				if(n.isEmpty)
					fn(None)
				else
					fn(Some(n.toInt))
			}
		}
	}
}

trait Registrar {
	def register(typeKey : String, default : Int)(fn : Int=>Aggregator[_]) {
		Registry.register(typeKey, default)(fn)
	}

	def register(typeKey : String)(fn : Int=>Aggregator[_]) {
		register(typeKey, 0)(fn)
	}

	def register(typeKey : String, agg: Aggregator[_]) {
		register(typeKey){n => agg}
	}

	def load {}
}
