package com.stripe.simmer

object Main {
	def main(args : Array[String]) {

		AlgebirdAggregators.load

		val capacity = if(args.size > 0) args(0).toInt else 5000
		val flushEvery = if(args.size > 1) args(1).toInt else 0

		val scrubber = new Simmer(StdOutput, capacity, flushEvery)

    	for(line <- io.Source.stdin.getLines) {
    		val columns = line.split("\t")
    		if(columns.size > 1)
	    		scrubber.update(columns(0), columns(1))
	   	}

	   	scrubber.flush
	   	System.exit(0)
	}
}

object StdOutput extends Output {
	def write[A](key : String, value : A, aggregator : Aggregator[A]) {
		println(key + "\t" + aggregator.serialize(value) + "\t" + aggregator.present(value))
	}
}
