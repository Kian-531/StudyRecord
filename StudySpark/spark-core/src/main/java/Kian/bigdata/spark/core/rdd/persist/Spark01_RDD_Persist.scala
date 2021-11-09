package Kian.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/19
 */
object Spark01_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    println("*************************************")

    val rdd1: RDD[String] = sc.makeRDD(list)

    val flatRDD1: RDD[String] = rdd1.flatMap(_.split(" "))
    val mapRDD1: RDD[(String, Int)] = flatRDD1.map((_, 1))
    val groupRDD = mapRDD1.groupBy(word => word)
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
