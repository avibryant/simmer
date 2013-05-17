package com.stripe.simmer

import com.twitter.algebird._
import com.twitter.bijection._
import com.twitter.chill._
import java.util.Calendar._
import java.util.GregorianCalendar

object AlgebirdAggregators extends Registrar {
	register("sum", DoubleSum)
	register("max", DoubleMax)
	register("min", DoubleMin)
	register("uv", 12){new HyperLogLog(_)}
	register("mh", 64){new MinHash(_)}
	register("pct", 50){new Percentile(_)}
	register("fh", 10){new HashingTrick(_)}
	register("dcy", 86400){new Decay(_)}
	registerRecursive("top", 10){(k,inner) => new HeavyHitters(k,inner)}
	registerRecursive("bot", 10){(k,inner) => new HeavyHitters(k,inner,-1.0)}
}

trait MonoidAggregator[A] extends Aggregator[A] {
	def monoid : Monoid[A]
	def reduce(left : A, right : A) = monoid.plus(left, right)
}

trait NumericAggregator[A] extends MonoidAggregator[A] {
	def presentNumeric(value : A) : Double
}

trait AlgebirdAggregator[A] extends MonoidAggregator[A] {
	val MAGIC = "%%%"

	def injection : Injection[A, String]

	def serialize(value : A) = MAGIC + injection(value)
	def deserialize(serialized : String) = {
		if(serialized.startsWith(MAGIC))
			injection.invert(serialized.drop(MAGIC.size))
		else
			None
	}
}

trait KryoAggregator[A] extends AlgebirdAggregator[A] {
  val injection : Injection[A,String] =
  	KryoInjection.asInstanceOf[Injection[A, Array[Byte]]] andThen
  	Bijection.bytes2Base64 andThen
  	Base64String.unwrap
}

trait DoubleAggregator extends NumericAggregator[Double] {
	def prepare(in : String) = in.toDouble
	def serialize(value : Double) = value.toString
	def deserialize(serialized : String) = Some(serialized.toDouble)
	def present(value : Double) = value.toString
	def presentNumeric(value : Double) = value
}

object DoubleSum extends DoubleAggregator {
	val monoid = implicitly[Monoid[Double]]
}

object DoubleMax extends DoubleAggregator {
	val monoid = new Monoid[Double] {
		val zero = Double.MinValue
		def plus(left : Double, right: Double) = left.max(right)
	}
}

object DoubleMin extends DoubleAggregator {
	val monoid = new Monoid[Double] {
		val zero = Double.MaxValue
		def plus(left : Double, right: Double) = left.min(right)
	}
}

class HyperLogLog(size : Int) extends KryoAggregator[HLL] with NumericAggregator[HLL] {
  val monoid = new HyperLogLogMonoid(size)
  def prepare(in : String) = monoid.create(in.getBytes)
  def present(out : HLL) = out.estimatedSize.toInt.toString
  def presentNumeric(out : HLL) = out.estimatedSize
}

class MinHash(hashes : Int) extends AlgebirdAggregator[Array[Byte]] {
	val monoid = new MinHasher16(0.1, hashes * 2)
	val injection = Bijection.bytes2Base64 andThen Base64String.unwrap
	def prepare(in : String) = monoid.init(in)
	def present(out : Array[Byte]) = {
		out.grouped(2).toList.map{h => h.map{"%02X".format(_)}.mkString}.mkString(":")
	}
}

class Top(k : Int) extends KryoAggregator[TopK[(Double,String)]] {
	val monoid = new TopKMonoid[(Double,String)](k)
	def prepare(in : String) = {
		val (score, item) = split(in, ":").get
		monoid.build((score.toDouble * -1, item))
	}

	def present(out : TopK[(Double,String)]) = {
		out.items.map{case (score,item) => (score * -1).toString + ":" + item}.mkString(",")
	}
}

class Percentile(pct : Int) extends KryoAggregator[QTree[Double]] with NumericAggregator[QTree[Double]]{
	val monoid = new QTreeSemigroup[Double](6) with Monoid[QTree[Double]] {
		val zero = QTree(0.0) //not actually right but should never be used?
	}

	def prepare(in : String) = QTree(in.toDouble)
	def present(out : QTree[Double]) = presentNumeric(out).toString
	def presentNumeric(out : QTree[Double]) = out.quantileBounds(pct.toDouble / 100)._2
}

class HashingTrick(bits : Int) extends KryoAggregator[AdaptiveVector[Double]] {
	val monoid = new HashingTrickMonoid[Double](bits)
	def prepare(in : String) = {
		if(in.contains(":")) {
			val (key, value) = split(in, ":").get
			monoid.init(key.getBytes, value.toDouble)
		} else {
			monoid.init(in.getBytes, 1.0)
		}
	}

	def present(out : AdaptiveVector[Double]) = {
		out.mkString(",")
	}
}

class Decay(halflife : Int) extends KryoAggregator[DecayedValue] with NumericAggregator[DecayedValue] {
	val monoid = DecayedValue.monoidWithEpsilon(0.000001)
	def prepare(in : String) = {
		val (timestamp, value) = split(in, ":").get
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

	def presentNumeric(out : DecayedValue) = {
		val adjusted = monoid.plus(out, DecayedValue.build(0.0, timestampAsOfEndOfDay, halflife.toDouble))
		adjusted.value
	}

	def present(out : DecayedValue) = presentNumeric(out).toString
}

class HeavyHitters[A](k : Int, inner : Aggregator[A], order : Double = 1.0) extends KryoAggregator[SketchMap[String, A]] {
	val innerNumeric = inner match {
		case in : NumericAggregator[A] => in
		case _ => error("top and bot require a numeric aggregation")
	}
	implicit val str2Bytes = (x : String) => x.getBytes
	implicit val innerMonoid = innerNumeric.monoid
	implicit val ordering = Ordering.by{a : A => innerNumeric.presentNumeric(a) * order}
	val monoid = new SketchMapMonoid[String,A](100,5,123456,k)

	def prepare(in : String) = {
		val (key, value) = split(in, ":").get
		monoid.create(key, inner.prepare(value))
	}

	def present(out : SketchMap[String, A]) = {
		out.heavyHitters.map{case (k,v) => k + ":" + inner.present(v)}.mkString(",")
	}
}
