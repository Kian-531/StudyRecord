package Kian.bigdata.spark.core.rdd.builder


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/15
 */
object Spark02_RDD_File {


  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)


    // TODO 创建RDD
    // 从文件中创建RDD，将文件中的数据作为处理的数据源
    // val rdd: RDD[String] = sc.textFile("datas/1.txt")

    // path路径可以是文件具体路径，也可以是目录路径(读取多个文件)
    // val rdd: RDD[String] = sc.textFile("datas")

    // path路径可以使用通配符*
    val rdd: RDD[String] = sc.textFile("datas/1*.txt")

    // path路径可以是分布式存储系统路径：HDFS
    // val rdd: RDD[String] = sc.textFile("hdfs://master:9000/")


    rdd.collect().foreach(println)


    // TODO 关闭环境
    sc.stop()


  }
}
