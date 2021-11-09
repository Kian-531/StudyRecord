package Kian.bigdata.spark.streaming


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer


/**
 * @author Kian
 * @date 2021/10/4
 */
object SparkStreaming10_MockData {
  def main(args: Array[String]): Unit = {


    // TODO 生成模拟数据
    // 格式：timestamp area city userid adid
    // 含义：时间戳     区域  城市  用户id  广告

    // Application => Kafka => SparkStreaming => Analysis


    // 创建配置对象
//    val prop = new Properties()
//    // 添加配置
//    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "master")
//    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
//    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//      "org.apache.kafka.common.serialization.StringSerializer")
//    val producer = new KafkaProducer[String, String](prop)


    while (true) {
      mockData.foreach(
        data => {
          // 向Kafka中生成数据
          //          val record = new ProducerRecord[String, String]("Kian", data)
          //          producer.send(record)
          print(data)
        }
      )
    }

  }

  // 生成模拟数据
  def mockData = {
    val list = ListBuffer[String]()
    val areaList = ListBuffer[String]("华东", "华南", "华北")
    val cityList = ListBuffer[String]("北京", "上海", "深圳")

    for (i <- 1 to 10) {
      val area: String = areaList(new Random().nextInt(3))
      val city: String = cityList(new Random().nextInt(3))
      val userid: Int = new Random().nextInt(6)
      val adid: Int = new Random().nextInt(6)
      list.append(s"${System.currentTimeMillis()}, $area, $city, $userid, $adid")

    }
    list
  }
}
