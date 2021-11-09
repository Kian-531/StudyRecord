package Kian.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.lang.System


/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming08_close {
  def main(args: Array[String]): Unit = {


    //    TODO 创建环境
    // StreamingContext配置需要两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示数据批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    // reduceByKeyAndWindow:当滑动幅度小于窗口范围时，可以采用增加数据和删除数据的方式来防止数据重复
    // 无需重复计算，提升性能
    val windowDS: DStream[(String, Int)] = {
      wordToOne.reduceByKeyAndWindow(
        (x: Int, y: Int) => {
          x + y
        },
        (x: Int, y: Int) => {
          x - y
        },
        Seconds(12), Seconds(9)
      )
    }

    ssc.start()


    // TODO 优雅的关闭

    // 如果想要关闭采集器，那么需要创建新的线程
    // 而且需要在第三方程序中增加关闭状态
    new Thread(
      new Runnable {
        override def run(): Unit = {
          /*
           优雅的关闭(stopGracefully = true)
           计算机节点不会再接受新的数据，而是将现有的数据处理完在关闭

           第三方状态例如
           MySQL: Table(stopSpark) => Row => data
           Redis: Data (K-V)
           ZK   : /stopSpark
           HDFS : /stopSpark
          */


          while (true) {
            // 逻辑判断第三方状态，如果为真，关闭
            if (true) {
              // 获取SparkStreaming状态
              val state: StreamingContextState = ssc.getState()
              if (state == StreamingContextState.ACTIVE) {
                ssc.stop(true, true)
              }
              Thread.sleep(5000)
            }
          }
          // 关闭线程
          System.exit(0)
        }
      }
    ).start()


    ssc.awaitTermination()
  }

}
