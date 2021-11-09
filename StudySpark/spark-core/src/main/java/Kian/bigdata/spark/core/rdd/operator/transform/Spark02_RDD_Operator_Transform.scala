package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark02_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)


    // mapPartitions: 可以以分区为单位进行数据转换操作
    //                会将整个分区的数据加载到内存中
    //                处理完的数据不会被立即释放掉，还存在对象的引用
    //                再内存较小，数据量较大的情况下，容易出现内存的溢出
    val mapRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println("<<<<<<<<<<<")
        iter.map(_ * 2)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}
