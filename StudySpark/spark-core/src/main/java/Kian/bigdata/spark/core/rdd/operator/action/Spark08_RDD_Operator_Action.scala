package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark08_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      2, 4, 3, 6, 5
    ), 2)

    // TODO - 行动算子 - aggregate
    // 第一个参数: 传入初始值
    //
    // 第二个参数: 第一个值表示分区内的计算，第二个值表示分区间的计算规则

    println(rdd.aggregate(1)(
      (x, y) => {
        math.max(x, y)
      },
      (x, y) => {
        x * y
      }
    ))


    sc.stop()

  }
}
