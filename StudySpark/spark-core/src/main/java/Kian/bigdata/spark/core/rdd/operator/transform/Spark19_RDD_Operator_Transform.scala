package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark19_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - key-value类型
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("a", 5), ("b", 6)
    ) )


    // join: 将两个不同数据源的数据，将相同key的value连接组成一个元组
    //       如果两个数据源中的key没有匹配上，那么数据不会出现在结果中
    //       如果相同的key有多个,会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长， 会导致性能降低
    rdd1.join(rdd2).collect().foreach(println)

    sc.stop()
  }
}
