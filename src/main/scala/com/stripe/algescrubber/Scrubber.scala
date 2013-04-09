package com.stripe.algescrubber

import java.util.{Map => JMap, LinkedHashMap => JLinkedHashMap}
import scala.collection.JavaConverters._

trait Output {
    def write[A](key : String, value : A, aggregator : Aggregator[A])
}

class Scrubber(output : Output, capacity : Int, flushEvery : Int) {
    
    val accumulators = new JLinkedHashMap[String,Accumulator[_]](capacity, 0.75f, true) {
        override def removeEldestEntry(eldest : JMap.Entry[String, Accumulator[_]]) = {
            if(this.size > capacity) {
                eldest.getValue.write(eldest.getKey, output)
                true
            } else {
                false
            }
        }
    }

    def update(key : String, value : String) {
        val acc = accumulators.get(key)
        if(acc == null) {
            Registry.get(key) match {
                case Some(agg) => {
                    val newAcc = agg.createAccumulator(value)
                    accumulators.put(key, newAcc)
                }
                case None => error("Could not find aggregator for key " + key)
            }
        } else {
            acc.update(value)
            if(flushEvery > 0 && acc.count >= flushEvery) {
                acc.write(key, output)
                accumulators.remove(key)
            }
        }
    }

    def flush {
        accumulators.asScala.foreach{case (key,acc) => acc.write(key, output)}
        accumulators.clear
    }
}

class Accumulator[A](aggregator : Aggregator[A], var value : A) {
    var count = 1

    def update(input : String) {
        val newValue = aggregator.deserialize(input).getOrElse(aggregator.prepare(input))
        value = aggregator.reduce(value, newValue)
        count += 1
    }

    def write(key : String, output : Output) {
        output.write(key, value, aggregator)
    }
}