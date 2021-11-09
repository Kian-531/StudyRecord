package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark17_RDD_Operator_Transform2 {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - key-value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)


    // rdd.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

    // 如果分区内和分区间的算法相同，可以简化
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    sc.stop()
  }
}
