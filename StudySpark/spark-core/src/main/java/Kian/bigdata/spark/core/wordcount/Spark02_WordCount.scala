package Kian.bigdata.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/12
 */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("datas")

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(
      word => {
        (word, 1)
      }
    )

    // Spark框架提供了一个功能，可以将分组和聚合使用一个方法实现
    // reduceByKey: 相同的key的数据可以根据value进行reduce聚合
    val wordCount = wordToOne.reduceByKey((x, y) => x + y)


    val array = wordCount.collect()
    array.foreach(println)

    sc.stop()

  }
}
