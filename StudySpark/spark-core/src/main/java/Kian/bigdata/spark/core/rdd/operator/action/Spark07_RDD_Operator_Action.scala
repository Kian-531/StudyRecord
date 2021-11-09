package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      2,4,3,6,5
    ), 2)

    // TODO - 行动算子 - takeOrdered
    // 返回RDD中排序后的前n个元素组成的数组(默认升序排列)

    rdd.takeOrdered(2)(Ordering.Int.reverse).foreach(println)


    sc.stop()

  }
}
