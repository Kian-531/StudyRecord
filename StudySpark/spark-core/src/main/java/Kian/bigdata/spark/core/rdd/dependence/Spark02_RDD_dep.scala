package Kian.bigdata.spark.core.rdd.dependence

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/19
 */
object Spark02_RDD_dep {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // dependencies: 打印依赖关系

    val lines = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("************************")

    val words = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("************************")

    val wordGroup = words.groupBy(word => word)
    println(wordGroup.dependencies)
    println("************************")

    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    println(wordToCount.dependencies)
    println("************************")

    val array = wordToCount.collect()
    array.foreach(println)


    sc.stop()
  }
}
