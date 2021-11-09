package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark21_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - key-value类型
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("c", 4), ("a", 5), ("b", 6)
    ))

    // cogroup: connect + group
    // 会将相同的key的value分成一组，如果没有，就返回nothing
    val coRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
    coRDD.collect().foreach(println)
    sc.stop()
  }
}
