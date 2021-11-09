package Kian.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Kian
 * @date 2021/8/24
 */
object Spark05_Bc {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("C", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("C", 6)
    ))

    val map = mutable.Map(("a", 4), ("b", 5), ("C", 6))

    // join只会导致数据量几何式增长，并且会影响shuffle的性能，不推荐使用
    //    rdd1.join(rdd2).collect().foreach(println)

    // 考虑数据转换，可以根据map来实现对应join功能
    //    (a, 1),   (b, 2),   (C, 3)
    //    (a, (1,4)), (b, (2,5)), (C, (3,6))
    val mapRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (w, c) => {
        val i: Int = map.getOrElse(w, 0)
        (w, (c, i))
      }
    }

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
