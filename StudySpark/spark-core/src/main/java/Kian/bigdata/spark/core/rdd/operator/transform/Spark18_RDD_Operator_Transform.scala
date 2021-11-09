package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark18_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - key-value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    // combineByKey有三个参数
    // 第一个参数表示：将相同key的第一个数据进行结构转换
    // 第二个参数表示：分区内的计算规则
    // 第三个参数表示：分区间的计算规则
    val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      (v: Int) => (v, 1),
      (t: (Int, Int), v: Int) => {
        (t._1 + v, t._2 + 1)
      },
      (t1: (Int, Int), t2: (Int, Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    newRDD.mapValues(t => t._1 / t._2).collect().foreach(println)

    sc.stop()
  }
}
