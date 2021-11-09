package Kian.bigdata.spark.core.rdd.partation

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/24
 */
object Spark01_RDD_Partition {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxxxxx"),
      ("cba", "xxxxxx"),
      ("wnba", "xxxxxx"),
      ("nba", "xxxxxx")
    ))

    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)

    partRDD.saveAsTextFile("part")

    sc.stop()

  }

  /**
   * 自定义分区器
   * 1. 继承Partitioner
   * 2. 重写方法
   */
  class MyPartitioner extends Partitioner {

    // 分区数量
    override def numPartitions: Int = 3

    // 自定义分区规则
    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }

  }

}
