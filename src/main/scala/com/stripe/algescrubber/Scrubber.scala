package com.stripe.algescrubber

trait Output {
    def write[A](valueKey : String, value : A, aggKey : String, aggregator : Aggregator[A])
}

class Scrubber(output : Output) {
    val tables = scala.collection.mutable.Map[String,Table[_]]()

    def get(aggKey : String) = {
        Registry.get(aggKey).map{agg => tables.getOrElseUpdate(aggKey, agg.createTable)}
    }

    def flush {
        tables.foreach{case (key,table) => table.flush(key, output)}
    }
}

class Table[A](aggregator : Aggregator[A]) {
    var accumulators = Map[String,A]()

    def update(key : String, value : String) {
        val right = aggregator.deserialize(value).getOrElse(aggregator.prepare(value))
        accumulators.get(key) match {
            case None => accumulators += key -> right
            case Some(left) => accumulators += key -> aggregator.reduce(left, right)
        }
    }

    def flush(aggKey : String, output : Output) {
        accumulators.foreach { case (key,value) => output.write(key, value, aggKey, aggregator)}
        accumulators = Map[String,A]()
    }
}