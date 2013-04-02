package com.stripe.algescrubber

import com.twitter.algebird._
import com.twitter.bijection._
import com.twitter.chill._

object AlgebirdAggregators extends Registrar {
	register("hll", 12){new HyperLogLog(_)}
	register("mh", 64){new MinHash(_)}
	register("top", 10){new Top(_)}
	register("hist", new Histogram)
	register("pct", 50){new Percentile(_)}
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
  val semigroup = new HyperLogLogMonoid(size)
  def prepare(in : String) = semigroup.create(in.getBytes)
  def present(out : HLL) = out.estimatedSize.toInt.toString
}

class MinHash(hashes : Int) extends AlgebirdAggregator[Array[Byte]] {
	val semigroup = new MinHasher16(0.1, hashes * 2)
	val injection = Bijection.bytes2Base64 andThen Base64String.unwrap
	def prepare(in : String) = semigroup.init(in)
	def present(out : Array[Byte]) = {
		out.grouped(2).toList.map{h => h.map{"%02X".format(_)}.mkString}.mkString(":")
	}
}

class Top(k : Int) extends KryoAggregator[TopK[(Double,String)]] {
	val semigroup = new TopKMonoid[(Double,String)](k)
	def prepare(in : String) = {
		val (score, item) = Main.split(in, ":").get
		semigroup.build((score.toDouble * -1, item))
	}

	def present(out : TopK[(Double,String)]) = {
		out.items.map{case (score,item) => (score * -1).toString + ":" + item}.mkString(",")
	}
}

class Histogram extends KryoAggregator[Map[Int,Int]] {
	val semigroup = implicitly[Semigroup[Map[Int,Int]]]
	def prepare(in : String) = Map(in.toInt -> 1)
	def present(out : Map[Int,Int]) = out.keys.toList.sorted.map{k => k.toString + ":" + out(k)}.mkString(",")
}

class Percentile(pct : Int) extends Histogram {
	override def present(out : Map[Int,Int]) = {
		val sum = out.values.sum
		val target = sum.toDouble * pct / 100
		val sortedKeys = out.keys.toList.sorted
		val cumulative = sortedKeys.scanLeft(0){(acc,k) => acc+out(k)}
		sortedKeys.zip(cumulative.tail).find{_._2 >= target}.map{_._1}.getOrElse(0).toString
	}
}