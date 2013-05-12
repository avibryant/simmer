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

trait Output {
    def write[A](key : String, value : A, aggregator : Aggregator[A])
}

trait Input {
    def run(simmer : Simmer)
}

class Simmer(output : Output, capacity : Int, flushEvery : Option[Int]) {

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
            if(flushEvery.isDefined && acc.count >= flushEvery.get) {
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
        val newValue = aggregator.parse(input)
        value = aggregator.reduce(value, newValue)
        count += 1
    }

    def write(key : String, output : Output) {
        output.write(key, value, aggregator)
    }
}