package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark10_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 2), ("b", 4), ("c", 3), ("b", 6), ("a", 5)
    ), 2)

    // TODO - 行动算子 - countByKey
    // 统计每种key的个数

    println(rdd.countByKey())


    sc.stop()

  }
}
