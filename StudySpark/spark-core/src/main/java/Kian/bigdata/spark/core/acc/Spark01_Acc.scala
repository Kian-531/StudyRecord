package Kian.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/24
 */
object Spark01_Acc {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(
      1, 2, 3, 4
    ))

    // reduce: 有分区内和分区间的计算逻辑
    //    val i: Int = rdd.reduce(_ + _)
    //    println(i)


    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )
    println(sum)


    sc.stop()
  }
}
