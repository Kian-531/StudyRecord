package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark14_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - key-value类型
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    // partitionBy:根据指定的分区规则对数据进行分区
    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")

    sc.stop()
  }
}
