package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark11_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - repartition(扩大分区)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // coalesce:缩减分区
    // repartition: 扩大分区，底层调用coalesce, shuffle默认是true(数据均衡)
    val rdd1: RDD[Int] = rdd.repartition(3)
    // val rdd1: RDD[Int] = rdd.coalesce(3, true)

    rdd1.saveAsTextFile("output")

    sc.stop()
  }
}
