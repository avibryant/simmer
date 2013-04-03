package com.stripe.algescrubber
import java.util.concurrent._
import scala.concurrent.ops._

class ParallelScrubber(shards : Int = 16) {
	val scrubbers = (1 to shards).map{i => new Scrubber}
	val queue = new ArrayBlockingQueue[(String,String,String)](10000)
	var count = 0

	scrubbers.foreach {scrubber =>
		spawn {
			while(true) {
				val (aggKey, valueKey, value) = queue.take
				scrubber.update(aggKey, valueKey, value)
			}
		}
	}

	def update(aggKey : String, valueKey : String, value : String) {
		count += 1
		if(count % 10000 == 0)
			System.err.println(count + " items, queue size " + queue.size)
		queue.put((aggKey, valueKey, value))
	}


	def flush(output : Output) {
		while(queue.size > 0)
			Thread.sleep(100)

		val mergeOutput = new MergeOutput
		scrubbers.foreach{_.flush(mergeOutput)}
		mergeOutput.flush(output)
	}
}

class MergeOutput extends Output {
	val scrubber = new Scrubber

    def write[A](valueKey : String, value : A, aggKey : String, aggregator : Aggregator[A]) {
    	scrubber.update(aggKey, valueKey, aggregator.serialize(value))
    }

    def flush(output: Output) {
    	scrubber.flush(output)
    }
}