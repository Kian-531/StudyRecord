package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark09_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      2, 4, 3, 6, 5
    ), 2)

    // TODO - 行动算子 - fold
    // fold有两个参数
    // 第一个参数是初始值
    // 第二个参数里面只有一个内容，是分区内核分区间的计算规则
    // 当aggregate中分区内和分区间的计算规则相同时，可以使用fold方法代替
    println(rdd.fold(0)(_ + _))


    sc.stop()

  }
}
