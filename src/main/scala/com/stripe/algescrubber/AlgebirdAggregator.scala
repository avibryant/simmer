package com.stripe.algescrubber

import com.twitter.algebird._
import com.twitter.bijection._
import com.twitter.chill._
import java.util.Calendar._
import java.util.GregorianCalendar

object AlgebirdAggregators extends Registrar {
	register("hll", 12){new HyperLogLog(_)}
	register("mh", 64){new MinHash(_)}
	register("top", 10){new Top(_)}
	register("hist", new Histogram)
	register("pct", 50){new Percentile(_)}
	register("hash", 10){new HashingTrick(_)}
	register("dcy", 86400){new Decay(_)}
	register("hh", 10){new HeavyHitters(_)}
	register("hhll", 10){new HeavyHittersHyperLogLog(_)}
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

class HashingTrick(bits : Int) extends KryoAggregator[AdaptiveVector[Double]] {
	val semigroup = new HashingTrickMonoid[Double](bits)
	def prepare(in : String) = {
		if(in.contains(":")) {
			val (key, value) = Main.split(in, ":").get
			semigroup.init(key.getBytes, value.toDouble)
		} else {
			semigroup.init(in.getBytes, 1.0)
		}
	}

	def present(out : AdaptiveVector[Double]) = {
		out.mkString(",")
	}
}

class Decay(halflife : Int) extends KryoAggregator[DecayedValue] {
	val semigroup = DecayedValue.monoidWithEpsilon(0.000001)
	def prepare(in : String) = {
		val (timestamp, value) = Main.split(in, ":").get
		DecayedValue.build(value.toDouble, timestamp.toDouble, halflife.toDouble)
	}

	def timestampAsOfEndOfDay = {
		val calendar = new GregorianCalendar
		calendar.add(DATE, 1)
    	calendar.set(HOUR_OF_DAY, 0)
    	calendar.set(MINUTE, 0)
    	calendar.set(SECOND, 0)
    	calendar.set(MILLISECOND, 0)
		calendar.getTimeInMillis / 1000
	}

	def present(out : DecayedValue) = {
		val adjusted = semigroup.plus(out, DecayedValue.build(0.0, timestampAsOfEndOfDay, halflife.toDouble))
		adjusted.value.toString
	} 
}

class HeavyHitters(k : Int) extends KryoAggregator[SketchMap[String, Int]] {
	implicit val str2Bytes = (x : String) => x.getBytes
	val semigroup = new SketchMapMonoid[String,Int](100,5,123456,k)

	def prepare(in : String) = semigroup.create(in, 1)
	def present(out : SketchMap[String, Int]) = {
		out.heavyHitters.map{case (k,v) => k + ":" + v.toString}.mkString(",")
	}
}

class HeavyHittersHyperLogLog(k : Int) extends KryoAggregator[SketchMap[String, HLL]] {
	implicit val str2Bytes = (x : String) => x.getBytes
	implicit val hllMonoid = new HyperLogLogMonoid(6)
	implicit val hllOrdering = Ordering.by[HLL,Double]{_.estimatedSize}
	val semigroup = new SketchMapMonoid[String,HLL](100,5,123456,k)

	def prepare(in : String) = {
		val (key, value) = Main.split(in, ":").get
		semigroup.create(key, hllMonoid.create(value))
	}

	def present(out : SketchMap[String, HLL]) = {
		out.heavyHitters.map{case (k,v) => k + ":" + v.estimatedSize.toInt.toString}.mkString(",")
	}
}