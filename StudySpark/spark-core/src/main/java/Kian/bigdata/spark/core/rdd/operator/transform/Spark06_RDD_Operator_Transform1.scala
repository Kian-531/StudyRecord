package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark06_RDD_Operator_Transform1 {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - groupBy
    val rdd: RDD[String] = sc.makeRDD(List("hello", "hadoop", "spark", "scala", "hive"), 2)

    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))


    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
