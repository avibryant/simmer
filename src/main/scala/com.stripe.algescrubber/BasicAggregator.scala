package com.stripe.algescrubber

object BasicAggregators extends Registrar {
	register("l", LongSum)
	register("lmax", LongMax)
	register("lmin", LongMin)
}

trait BasicAggregator[A] extends Aggregator[A] {
	def serialize(value : A) = present(value)
	def deserialize(input : String) = Some(prepare(input))
}

trait LongAggregator extends BasicAggregator[Long] {
	def prepare(input : String) = input.toLong
	def present(value : Long) = value.toString
}

object LongSum extends LongAggregator {
	def reduce(left : Long, right : Long) = left + right
}

object LongMax extends LongAggregator {
	def reduce(left : Long, right : Long) = left.max(right)
}

object LongMin extends LongAggregator {
	def reduce(left : Long, right : Long) = left.min(right)
}

