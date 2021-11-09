package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark04_RDD_Operator_Transform2 {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - flatMap
    //
    val rdd: RDD[Any] = sc.makeRDD(
      List(
        List(1, 2), 3, List(4, 5)
      )
    )

    val flatRDD: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[Int] => list
          case dat => List(dat)
        }
      }
    )

    flatRDD.collect().foreach(println)

    sc.stop()
  }
}
