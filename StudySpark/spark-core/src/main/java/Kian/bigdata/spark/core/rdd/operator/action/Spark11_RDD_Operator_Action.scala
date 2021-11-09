package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark11_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 2), ("a", 2), ("c", 3), ("b", 6), ("a", 3)
    ), 2)

    // TODO - 行动算子 - countByValue
    // 统计每种value的个数

    println(rdd.countByValue())
    // Map((a,3) -> 1, (b,6) -> 1, (a,2) -> 2, (c,3) -> 1)


    sc.stop()

  }
}
