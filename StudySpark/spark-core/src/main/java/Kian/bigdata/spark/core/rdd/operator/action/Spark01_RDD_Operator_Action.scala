package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      2,4,3,6,5
    ))

    // TODO - 行动算子
    // 所谓的行动算子，就是出发了作业(job)执行的方法
    // 行动算子直接就会出结果，转换算子每次运行会形成新的RDD


    rdd.collect()


    sc.stop()

  }
}
