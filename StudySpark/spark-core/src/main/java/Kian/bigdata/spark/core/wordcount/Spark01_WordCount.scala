package Kian.bigdata.spark.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/7/22-19:11
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // Application
    // Spark

    // TODO 建立和Spark框架的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount") //spark配置信息
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    // TODO 执行业务操作
    // 1. 读取文件，获取一行一行的数据
    //    “hello world”
    val lines = sc.textFile("datas")

    // 2. 将一行数据进行拆分，形成一个一个的单词(分词)
    // 扁平化，将整体拆分成个体的操作
    //    “hello world” => hello, world, hello, world
    val words = lines.flatMap(_.split(" "))

    // 3. 将数据根据相同的单词进行分组，便于统计
    //    (hello, hello), (world, world)
    val wordGroup = words.groupBy(word => word)

    // 4. 将分组后的数据进行转换
    //    (hello, 2), (world, 2)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 5. 将转换结果采集到控制台打印出来
    val array = wordToCount.collect()
    array.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }
}
