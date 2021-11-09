package Kian.bigdata.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/15
 */
object Spark01_RDD_Memory_par {


  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 创建RDD
    // RDD的并行度 & 分区
    // makeRDD可以传入两个参数，第二个参数表示分区的数量
    // 默认分区数是核心数
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 4)
    rdd.saveAsTextFile("output")

    // TODO 关闭环境
    sc.stop()


  }
}
