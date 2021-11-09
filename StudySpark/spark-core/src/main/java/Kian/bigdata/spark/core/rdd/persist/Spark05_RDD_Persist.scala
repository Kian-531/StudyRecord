package Kian.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/19
 */
object Spark05_RDD_Persist {
  def main(args: Array[String]): Unit = {

    // cache: 临时将数据保存在内存中重用
    // persist: 将数据临时存储在磁盘中进行数据重用
    //          涉及到磁盘IO，性能较低，但是数据安全
    //          如果作业执行完毕，临时保存的数据文件就会丢失
    // checkpoint: 将数据长久的保存在磁盘中进行数据重用
    //             涉及到磁盘IO，性能较低，但是数据安全
    //             为了保证数据安全，所以一般情况下，会独立执行作业(在执行一遍)
    //             为了提高效率，一般情况下需要和cache配合使用


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("hello scala", "hello spark")
    val rdd: RDD[String] = sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("@@@@@@@@@@@@@@@@")
        (word, 1)
      })


    mapRDD.cache()
    mapRDD.checkpoint()
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    println("*************************************")


    // 直接读取缓存的数据
    val groupRDD = mapRDD.groupBy(word => word)
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
