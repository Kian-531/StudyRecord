package Kian.bigdata.spark.streaming


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.net.URI


/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming08_close_example {
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
    new MonitorStop(ssc)
    ssc.awaitTermination()
  }

  // TODO 优雅的关闭第三方状态示例
  class MonitorStop(ssc: StreamingContext) extends Runnable {

    override def run(): Unit = {

      val fs: FileSystem = FileSystem.get(new URI("hdfs://linux1:9000"), new
          Configuration(), "atguigu")
      while (true) {
        try
          Thread.sleep(5000) catch {
          case e: InterruptedException => e.printStackTrace()
        }
        val state: StreamingContextState = ssc.getState
        val bool: Boolean = fs.exists(new Path("hdfs://linux1:9000/stopSpark"))
        if (bool) {
          if (state == StreamingContextState.ACTIVE) {
            ssc.stop(stopSparkContext = true, stopGracefully = true)
            System.exit(0)
          }
        }
      }
    }
  }

}
