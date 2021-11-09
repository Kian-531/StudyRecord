package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark03_RDD_Operator_Transform1 {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - mapPartitionsWithIndex
    // 获取数据来自哪个分区
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mpiRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      // 1   2   3   4
      // (0, 1) (1, 2) (4, 3) (6, 4)
      (index, iter) => {
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mpiRDD.collect().foreach(println)

    sc.stop()
  }
}
