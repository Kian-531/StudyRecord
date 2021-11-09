package Kian.bigdata.spark.core.rdd.dependence

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/19
 */
object Spark01_RDD_dep {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)


    // toDebugString: 打印血缘关系

    val lines = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("************************")

    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("************************")

    val wordGroup = words.groupBy(word => word)
    println(wordGroup.toDebugString)
    println("************************")

    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    println(wordToCount.toDebugString)
    println("************************")

    val array = wordToCount.collect()
    array.foreach(println)


    sc.stop()
  }
}
