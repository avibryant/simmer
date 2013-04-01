package com.stripe.algescrubber

object Main {
	def main(args : Array[String]) {

		BasicAggregators.load
		AlgebirdAggregators.load

		val scrubber = new Scrubber(StdOutput)

    	for(line <- io.Source.stdin.getLines) {
    		for((fullKey, value) <- split(line, "\t")) {
	    		for((aggKey, valueKey) <- split(fullKey, ":")) {
		    		scrubber.get(aggKey) match {
		    			case Some(table) => table.update(valueKey, value)
		    			case None => error("Could not find aggregator of type " + aggKey)
		    		}
		    	}
		    }
	   	}

	   	scrubber.flush
	}

	def split(str : String, delim : String) = {
        val parts = str.split(delim)
        val head = parts.head
        if(parts.size > 1 && head.size > 0)
            Some((head, str.drop(head.size + 1)))
        else
            None
	}
}

object StdOutput extends Output {
	def write[A](valueKey : String, value : A, aggKey : String, aggregator : Aggregator[A]) {
		println(aggKey + ":" + valueKey + "\t" + aggregator.serialize(value))
		println(":" + aggKey + ":" + valueKey + "\t" + aggregator.present(value))
	}
}