package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark10_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - coalesce(缩减分区)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // 第一个参数表示，缩减后的分区数量
    // 第二个参数表示是否进行shuffle处理(数据倾斜是使用)
    val coalesceRDD: RDD[Int] = rdd.coalesce(2, true)
    coalesceRDD.saveAsTextFile("output1")

    sc.stop()
  }
}
