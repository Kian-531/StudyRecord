package Kian.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming06_state_transform {
  def main(args: Array[String]): Unit = {


    //    TODO 创建环境
    // StreamingContext配置需要两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示数据批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // transform可以将底层rdd获取到后进行操作
    // 1. DStream功能不完善的时候
    // 2. 需要代码周期性的执行


    // Code: Driver端
    val newDS: DStream[String] = lines.transform(
      rdd => {
        // Driver端，（周期性执行）
        rdd.map(
          str => {
            // Executor端
            str
          }
        )
      }
    )

    // Code: Driver端
    val newDS1: DStream[String] = lines.map(
      data => {
        // Executor端
        data
      }
    )

    ssc.start()

    ssc.awaitTermination()
  }

}
