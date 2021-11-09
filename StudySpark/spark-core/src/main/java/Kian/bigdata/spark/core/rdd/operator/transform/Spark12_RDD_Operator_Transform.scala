package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark12_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - sortBy
    val rdd: RDD[Int] = sc.makeRDD(List(6, 3, 5, 4, 2, 1), 2)

    val sortRDD: RDD[Int] = rdd.sortBy(num => num)
    sortRDD.collect().foreach(println)
//    sortRDD.saveAsTextFile("output")


    sc.stop()
  }
}
