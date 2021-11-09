package Kian.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/19
 */
object Spark02_RDD_Persist {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("@@@@@@@@@@@@@@@@")
        (word, 1)
      })

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    println("*************************************")


    // RDD中不存储数据，因此再次使用时，会先执行之前的操作(读取原数据然后到这一步)
    val groupRDD = mapRDD.groupBy(word => word)
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
