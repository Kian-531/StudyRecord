package Kian.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming05_state {
  def main(args: Array[String]): Unit = {


    //    TODO 创建环境
    // StreamingContext配置需要两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示数据批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    // 无状态数据操作，只对当前的采集周期内的数据进行操作
    // 在某些场合下，需要保留数据操作结果(状态)，实现数据的汇总
    // 使用有状态操作时需要设定检查点路径
    val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)


    val wordToOne: DStream[(String, Int)] = datas.map((_, 1))

    //    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    // updateStateByKey:根据key对数据的状态进行更新
    // 传递的参数有两个值
    // 第一个表示相同key的value数据(seq)
    // 第二个表示缓冲区相同key的value数据(buff)
    //    缓冲区里面有可能有值也有可能没值
    val state: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount: Int = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )

    //    wordToCount.print()
    state.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
