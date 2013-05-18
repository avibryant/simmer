package com.stripe.simmer

import java.util.{Map => JMap, LinkedHashMap => JLinkedHashMap}
import scala.collection.JavaConverters._

trait Aggregator[A] {
    def createAccumulator(input : String) = new Accumulator(this, parse(input))
    def parse(input : String) : A = deserialize(input).getOrElse(prepare(input))
    def reduce(left : A, right : A) : A
    def prepare(input : String) : A
    def serialize(value : A) : String
    def deserialize(serialized : String) : Option[A]
    def present(value : A) : String
}

class Simmer(output : Output, capacity : Int, flushEvery : Option[Int]) {

    Runtime.getRuntime.addShutdownHook(new Thread { override def run { flush } })

    val accumulators = new JLinkedHashMap[String,Accumulator[_]](capacity, 0.75f, true) {
        override def removeEldestEntry(eldest : JMap.Entry[String, Accumulator[_]]) = {
            if(this.size > capacity) {
                eldest.getValue.write(eldest.getKey, output)
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
            if(flushEvery.isDefined && acc.count >= flushEvery.get) {
                if(acc.write(key, output)) {
                    accumulators.remove(key)
                }
            }
        }
    }

    def flush {
        //TODO respect the return value from write()
        accumulators.asScala.foreach{case (key,acc) => acc.write(key, output)}
        accumulators.clear
    }
}

class Accumulator[A](aggregator : Aggregator[A], var value : A) {
    var count = 1

    def update(input : String) {
        value = merge(input)
        count += 1
    }

    def merge(input : String) = {
        val newValue = aggregator.parse(input)
        aggregator.reduce(value, newValue)
    }

    def mergeAndPresent(input : String) = {
        aggregator.present(merge(input))
    }

    def present = aggregator.present(value)

    def write(key : String, output : Output) = {
        output.write(key, value, aggregator)
    }
}