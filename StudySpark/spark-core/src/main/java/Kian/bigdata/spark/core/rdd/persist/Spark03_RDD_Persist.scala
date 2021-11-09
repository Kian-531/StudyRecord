package Kian.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/19
 */
object Spark03_RDD_Persist {
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

    // cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到磁盘中，需要设置存储级别
    //    mapRDD.cache()
    // 持久化操作不止用于数据重用，也可以有效提高效率(经过繁琐操作后的数据可以在中途保存一份)


    // 持久化必须在行动算子执行时完成的
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    println("*************************************")


    // 直接读取缓存的数据
    val groupRDD = mapRDD.groupBy(word => word)
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
