package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark08_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - sample
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // sample算子需要传递三个参数
    // 1. 第一个参数表示，抽取后是否放回数据 true(放回) false(不放回)
    // 2. 第二个参数表示，数据源中每一个数据被抽取到的概率
    // 3. 第三个参数表示，抽取数据是随机算法的种子(随机种子确定，既每一个数据被抽到的概率也就确定)
    //                 不传参数的时候，默认是当前系统时间
    val sampleRDD: RDD[Int] = rdd.sample(false, 0.4, 1)


    sampleRDD.collect().foreach(println)


    sc.stop()
  }
}
