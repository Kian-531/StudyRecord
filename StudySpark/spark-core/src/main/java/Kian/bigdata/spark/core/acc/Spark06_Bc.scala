package Kian.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Kian
 * @date 2021/8/24
 */
object Spark06_Bc {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("C", 3)
    ))

    val map = mutable.Map(("a", 4), ("b", 5), ("C", 6))

    // 广播变量：每一个Executor中存在一份，每一个Task可以读取(共享可读)
    //         广播变量不可变
    //         需要大量数据进行类似join操作时，提高性能


    // 封装广播变量
    // Task可根据需要读取广播变量的数据
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    val mapRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (w, c) =>
        // 访问广播变量
        val i: Int = bc.value.getOrElse(w, 0)
        (w, (c, i))
    }

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
