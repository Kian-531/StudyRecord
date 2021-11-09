package Kian.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming06_state_join {
  def main(args: Array[String]): Unit = {


    //    TODO 创建环境
    // StreamingContext配置需要两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示数据批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    /*
    * 两个流之间的 join 需要两个流的批次大小一致，这样才能做到同时触发计算。计算过程就是
    * 对当前批次的两个流中各自的 RDD 进行 join，与两个 RDD 的 join 效果相同。
    * */

    val data1: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val data2: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

    val mapData1: DStream[(String, Int)] = data1.map((_, 1))
    val mapData2: DStream[(String, Int)] = data2.map((_, 1))

    // 所谓两个DStream的join操作就是两个rdd的join操作
    val joinDS: DStream[(String, (Int, Int))] = mapData1.join(mapData2)

    joinDS.print()
    ssc.start()

    ssc.awaitTermination()
  }

}
