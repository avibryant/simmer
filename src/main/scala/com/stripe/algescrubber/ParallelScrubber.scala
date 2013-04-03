package com.stripe.algescrubber
import java.util.concurrent._
import scala.concurrent.ops._

class ParallelScrubber(nShards : Int = 16) {
	val shards = (1 to nShards).map{i => new Shard}
	var count = 0
	val rand = new scala.util.Random

	def update(aggKey : String, valueKey : String, value : String) {
		count += 1
//		if(count % 10000 == 0)
//			System.err.println(count + " items")

    	System.err.println("updating " + valueKey)

		val mainShardIndex = (valueKey.hashCode % nShards + nShards) % nShards
		if(!shards(mainShardIndex).queue.offer((aggKey, valueKey, value)))
			shards(rand.nextInt(nShards)).queue.put((aggKey, valueKey, value))
	}


	def flush(output : Output) {
		while(shards.exists{_.queue.size > 0})
			Thread.sleep(100)

		System.err.println("flushing")

		val mergeOutput = new MergeOutput
		shards.foreach{_.scrubber.flush(mergeOutput)}
		mergeOutput.flush(output)
	}
}

class Shard {
	val queue = new ArrayBlockingQueue[(String,String,String)](1000)
	val scrubber = new Scrubber

	spawn {
		while(true) {
			val (aggKey, valueKey, value) = queue.take
			scrubber.update(aggKey, valueKey, value)
		}
	}
}


class MergeOutput extends Output {
	val scrubber = new Scrubber

    def write[A](valueKey : String, value : A, aggKey : String, aggregator : Aggregator[A]) {
    	System.err.println("merging " + valueKey)
    	scrubber.update(aggKey, valueKey, aggregator.serialize(value))
    }

    def flush(output: Output) {
    	scrubber.flush(output)
    }
}