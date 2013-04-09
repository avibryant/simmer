package com.stripe.algescrubber

object Registry {	
	var registry = Map[String,(Option[Int],Option[Aggregator[_]])=>Aggregator[_]]()
	var cache = Map[String,Aggregator[_]]()

	def register(typeKey : String)(fn : (Option[Int], Option[Aggregator[_]])=>Aggregator[_]) {
		registry += typeKey -> fn
	}

	//this is a bit of a mess
	def get(key: String) : Option[Aggregator[_]] = {
		val keyRegex = """([a-zA-Z]+)(\d*)(:[a-zA-Z0-9:]+)?:[a-zA-Z0-9]+""".r

		keyRegex.findFirstMatchIn(key).flatMap{m =>
			val typeKey = m.group(1)
			val optionalInt = m.group(2)
			val optionalRecursion = if(m.group(3) == null) "" else m.group(3).tail
			val fullTypeKey = typeKey + optionalInt + optionalRecursion

			cache.get(fullTypeKey).orElse{
				val created = createAggregator(typeKey, optionalInt, optionalRecursion)
				if(created.isDefined)
					cache += fullTypeKey -> created.get
				created
			}
		}
	}

	def createAggregator(typeKey : String, optionalInt : String, optionalRecursion : String) = {
		val int = if(optionalInt.isEmpty) None else Some(optionalInt.toInt)
		val inner = if(optionalRecursion.isEmpty) None else get(optionalRecursion + ":dummy")

		registry.get(typeKey).map {fn => fn(int, inner)}
	}
}

trait Registrar {
	def register(typeKey : String, agg: Aggregator[_]) {
		register(typeKey, 0){n => agg}
	}

	def register(typeKey : String, default : Int)(fn : Int=>Aggregator[_]) {
		Registry.register(typeKey){(optInt, optRec) => fn(optInt.getOrElse(default))}
	}

	def registerRecursive(typeKey : String, default : Int)(fn : (Int,Aggregator[_])=>Aggregator[_]) {
		Registry.register(typeKey){
			(optInt, optRec) =>
			fn(optInt.getOrElse(default), optRec.getOrElse(error(typeKey + " requires a nested aggregation")))
		}
	}

	def load {}
}
