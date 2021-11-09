package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark16_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - key-value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))

    // val groupRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()


    groupRDD.collect().foreach(println)


    sc.stop()
  }
}
