package Kian.bigdata.spark.core.rdd.io


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/24
 */
object Spark02_RDD_IO_Load {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.textFile("output1")
    println(rdd.collect().mkString(","))

    val rdd1: RDD[(String, Int)] = sc.objectFile[(String,Int)]("output2")
    println(rdd1.collect().mkString(","))

    val rdd2= sc.sequenceFile[String,Int]("output3")
    println(rdd2.collect().mkString(","))


    sc.stop()

  }

}
