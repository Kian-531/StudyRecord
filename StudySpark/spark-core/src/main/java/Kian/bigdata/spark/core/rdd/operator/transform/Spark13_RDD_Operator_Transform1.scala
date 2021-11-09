package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark13_RDD_Operator_Transform1 {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - 双value类型
    // Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    // 两个数据源的分区数要保持一致
    // Can only zip RDDs with same number of elements in each partition
    // 两个分区数据源中的数据数量要保持一致
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    // 拉链
    // 拉链操作两个数据源的类型可以不一致
    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd3.collect().mkString(","))


    sc.stop()
  }
}
