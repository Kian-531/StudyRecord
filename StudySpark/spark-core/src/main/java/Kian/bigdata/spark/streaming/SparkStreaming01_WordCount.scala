package Kian.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming01_WordCount {
  def main(args: Array[String]): Unit = {


    //    TODO 创建环境

    // StreamingContext配置需要两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示数据批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //    TODO 逻辑处理

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
//    val words: DStream[String] = lines.flatMap(_.split(" "))
//    val wordToOne: DStream[(String, Int)] = words.map(
//      words => {
//        (words, 1)
//      }
//    )
//    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
//    wordToCount.print()


    //    TODO 关闭环境

ssc.awaitTermination()
  }

}
