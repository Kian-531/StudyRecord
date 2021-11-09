package Kian.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Kian
 * @date 2021/8/24
 */
object Spark03_Acc {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(
      1, 2, 3, 4
    ))


    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    val mapRDD: RDD[Unit] = rdd.map(
      num => {
        // 使用累加器
        sumAcc.add(num)
      }
    )


    // 获取累加器的值
    // 少加: 转换算子中调用累加器，如果没有行动算子的话就不会执行
    // 多加: 转换算子中调用累加器，如果没有行动算子的话就不会执行
    // 一般情况下，累加器会犯在行动算子中执行
    mapRDD.collect()
    mapRDD.collect()


    println(sumAcc.value)
    sc.stop()
  }
}
