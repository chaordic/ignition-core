package ignition.core.jobs.utils

import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import scalaz.{Success, Validation}

import scala.collection.mutable
import scala.reflect._
import scala.util.Random

object RDDUtils {

  private lazy val logger = LoggerFactory.getLogger("ignition.core.RDDUtils")

  //TODO: try to make it work for any collection
  implicit class OptionRDDImprovements[V: ClassTag](rdd: RDD[Option[V]]) {
    def flatten: RDD[V] = {
      rdd.flatMap(x => x)
    }
  }

  implicit class SeqRDDImprovements[V: ClassTag](rdd: RDD[Seq[V]]) {
    def flatten: RDD[V] = {
      rdd.flatMap(x => x)
    }
  }

  implicit class SetRDDImprovements[V: ClassTag](rdd: RDD[Set[V]]) {
    def flatten: RDD[V] = {
      rdd.flatMap(x => x)
    }
  }

  implicit class ValidatedRDDImprovements[A: ClassTag, B: ClassTag](rdd: RDD[Validation[A, B]]) {

    def mapSuccess(f: B => Validation[A, B]): RDD[Validation[A, B]] = {
      rdd.map({
        case Success(v) => f(v)
        case failure => failure
      })
    }
  }

  implicit class ValidatedPairRDDImprovements[A: ClassTag, B: ClassTag, K: ClassTag](rdd: RDD[(K, Validation[A, B])]) {

    def mapValuesSuccess(f: B => Validation[A, B]): RDD[(K, Validation[A, B])] = {
      rdd.mapValues({
        case Success(v) => f(v)
        case failure => failure
      })
    }
  }

  implicit class RDDImprovements[V: ClassTag](rdd: RDD[V]) {
    def filterNot(p: V => Boolean): RDD[V] = rdd.filter(!p(_))
  }

  implicit class PairRDDImprovements[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

    def flatMapPreservingPartitions[U: ClassTag](f: ((K, V)) => Seq[U]): RDD[(K, U)] = {
      rdd.mapPartitions[(K, U)](kvs => {
        kvs.flatMap[(K,U)](kv => Stream.continually(kv._1) zip f(kv))
      }, preservesPartitioning = true)
    }

    def mapPreservingPartitions[U: ClassTag](f: ((K, V)) => U): RDD[(K, U)] = {
      rdd.mapPartitions[(K, U)](kvs => {
        kvs.map[(K,U)](kv => (kv._1, f(kv)))
      }, preservesPartitioning = true)
    }

    def collectValues[U: ClassTag](f: PartialFunction[V, U]): RDD[(K, U)] = {
      rdd.filter { case (k, v) => f.isDefinedAt(v) }.mapValues(f)
    }

    // loggingFactor: percentage of the potential logging that will be really printed
    // Big jobs will have too much logging and my eat up cluster disk space
    def groupByKeyAndTake(n: Int, loggingFactor: Double = 0.5): RDD[(K, List[V])] =
      rdd.aggregateByKey(mutable.ListBuffer.empty[V])(
        (lst, v) =>
          if (lst.size >= n) {
            if (Random.nextDouble() < loggingFactor)
              logger.warn(s"Ignoring value '$v' due aggregation result of size '${lst.size}' is bigger than n=$n")
            lst
          } else {
            lst += v
            lst
          },
        (lstA, lstB) =>
          if (lstA.size >= n)
            lstA
          else if (lstB.size >= n)
            lstB
          else {
            if (lstA.size + lstB.size > n) {
              if (Random.nextDouble() < loggingFactor)
                logger.warn(s"Merging partition1=${lstA.size} with partition2=${lstB.size} and taking the first n=$n, sample1='${lstA.take(5)}', sample2='${lstB.take(5)}'")
              lstA ++= lstB
              lstA.take(n)
            } else {
              lstA ++= lstB
              lstA
            }
          }
      ).mapValues(_.toList)

    // Note: completely unoptimized. We could use instead for better performance:
    // 1) sortByKey
    // 2) something like: http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/MinMaxPriorityQueue.html
    // 3) Dig in the implementation details and modify the Aggregator or ShuffleRDD to sort and limit data
    def groupByKeyAndTakeOrdered[B >: V](n: Int)(implicit ord: Ordering[B]): RDD[(K, List[V])] = {
      rdd.aggregateByKey(List.empty[V])(
        (lst, v) => (v :: lst).sorted(ord).take(n),
        (lstA, lstB) => (lstA ++ lstB).sorted(ord).take(n))
    }
  }
}