package Kian.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/19
 */
object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {
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

    // checkpoint需要罗盘，指定保存路径
    // 检查点路径保存的文件，当作业执行完毕后不会删除
    // 一般保存在分布式存储系统中，HDFS等
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
