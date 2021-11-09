package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark17_RDD_Operator_Transform3 {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - key-value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)


    // aggregateByKey最终返回的数据类型和给定的初始值类型一致
    // 获取形同key的数据平均值 => (a,3)(b,4)
    // (sameKeySum,sameKeyCount)
    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        println(s"t._1 = ${t._1},t._2 = ${t._2}, v = $v")
        println()
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    newRDD.mapValues(t => t._1 / t._2).collect().foreach(println)

    sc.stop()
  }
}
