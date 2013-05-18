package com.stripe.simmer

import com.twitter.util.Future

trait Input {
    def run(simmer : Simmer)
}

trait Output {
    def write[A](key : String, value : A, aggregator : Aggregator[A]) : Boolean
}

trait Lookup {
  def read(key : String) : Future[Option[(String,String)]]
}


object StdInput extends Input {
  def run(simmer : Simmer) {
    for(line <- io.Source.stdin.getLines) {
      val columns = line.split("\t")
      if(columns.size > 1)
        simmer.update(columns(0), columns(1))
    }

    System.exit(0)
  }
}

object StdOutput extends Output {
  def write[A](key : String, value : A, aggregator : Aggregator[A]) = {
    println(key + "\t" + aggregator.serialize(value) + "\t" + aggregator.present(value))
    true
  }
}

object NullLookup extends Lookup {
  def read(key : String) = Future.value(None)
}