package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark12_RDD_Operator_Transform1 {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - sortBy
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    // sortBy:第二个参数设置排序方式，默认是升序(true)
    // sortBy不会改变分区，中间会进行shuffle操作
    val sortRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt, false)
    sortRDD.collect().foreach(println)

    sc.stop()
  }
}
