package com.stripe.algescrubber

import java.util.HashMap
import scala.collection.JavaConverters._

trait Output {
    def write[A](valueKey : String, value : A, aggKey : String, aggregator : Aggregator[A])
}

class Scrubber {
    val tables = scala.collection.mutable.Map[String,Table[_]]()

    def get(aggKey : String) = {
        Registry.get(aggKey).map{agg => tables.getOrElseUpdate(aggKey, agg.createTable)}
    }

    def update(aggKey : String, valueKey : String, value : String) {
        get(aggKey) match {
            case Some(table) => table.update(valueKey, value)
            case None => error("Could not find aggregator of type " + aggKey)
        }
    }

    def flush(output : Output) {
        tables.foreach{case (key,table) => table.flush(key, output)}
    }
}

class Table[A](aggregator : Aggregator[A]) {
    var accumulators = new HashMap[String,A]()

    def update(key : String, value : String) {
        val right = aggregator.deserialize(value).getOrElse(aggregator.prepare(value))
        if(accumulators.containsKey(key))
            accumulators.put(key, aggregator.reduce(accumulators.get(key), right))
        else
            accumulators.put(key, right)
    }

    def flush(aggKey : String, output : Output) {
        accumulators.asScala.foreach { case (key,value) => output.write(key, value, aggKey, aggregator)}
    }
}