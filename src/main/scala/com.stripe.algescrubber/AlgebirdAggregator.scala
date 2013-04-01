package com.stripe.algescrubber

import com.twitter.algebird._
import com.twitter.bijection._
import com.twitter.chill._

object AlgebirdAggregators extends Registrar {
	register("hll", 12){n => new HyperLogLog(n)}
}

trait AlgebirdAggregator[A] extends Aggregator[A] {
	val MAGIC = "%%%"

	def semigroup : Semigroup[A]
	def injection : Injection[A, String]

	def serialize(value : A) = MAGIC + injection(value)
	def deserialize(serialized : String) = {
		if(serialized.startsWith(MAGIC))
			injection.invert(serialized.drop(MAGIC.size))
		else
			None
	}

	def reduce(left : A, right : A) = semigroup.plus(left, right)
}

trait KryoAggregator[A] extends AlgebirdAggregator[A] {
  val injection : Injection[A,String] = 
  	KryoInjection.asInstanceOf[Injection[A, Array[Byte]]] andThen
  	Bijection.bytes2Base64 andThen
  	Base64String.unwrap
}

class HyperLogLog(size : Int) extends KryoAggregator[HLL] {
  def semigroup = new HyperLogLogMonoid(size)
  def prepare(in : String) = semigroup.create(in.getBytes)
  def present(out : HLL) = out.estimatedSize.toInt.toString
}