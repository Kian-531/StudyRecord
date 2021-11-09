package Kian.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming06_state_window {
  def main(args: Array[String]): Unit = {


    //    TODO 创建环境
    // StreamingContext配置需要两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示数据批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 窗口的范围是采集周期的整数倍
    // 窗口是可以滑动的，默认情况下是按照一个采集周期滑动的
    //    这样的情况下可能会出现重复数据
    // 可以给窗口设置滑动周期，防止数据重复(滑动周期应大于或等于窗口的范围)
    val windowDS: DStream[String] = lines.window(Seconds(6), Seconds(6))

    val wordToOne: DStream[(String, Int)] = windowDS.map((_, 1))

    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    wordToCount.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
