package Kian.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}


/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming09_resume {
  def main(args: Array[String]): Unit = {


    //    TODO 创建环境
    // 从检查点获取数据来恢复，如果没有的话就创建
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))

      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

      val wordToOne: DStream[(String, Int)] = lines.map((_, 1))
      ssc
    }
    )

    // 创建检查点
    ssc.checkpoint("cp")
    ssc.start()


    // TODO 恢复数据


    ssc.awaitTermination()
  }

}
