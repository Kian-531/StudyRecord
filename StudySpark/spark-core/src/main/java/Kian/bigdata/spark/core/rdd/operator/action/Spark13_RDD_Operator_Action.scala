package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark13_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      2, 4, 3, 5, 4, 6
    ), 2)

    // TODO - 行动算子 - foreach

    // 收集后打印
    // foreach是Driver端的打印
    rdd.collect().foreach(println)

    println("===================")

    // 分布式打印
    // foreach是Executor端的打印
    // 没有顺序概念
    rdd.foreach(println)

    // 算子 : Operator (操作)
    // RDD的方法和Scala集合对象的方法不一样
    // RDD的方法可以将计算逻辑发送到Executor(分布式节点)端执行
    // 为了区分Scala的方法，将RDD的方法称之为算子
    // RDD的方法外部实在Driver端执行，方法内部的逻辑代码实在Executor端执行


    sc.stop()

  }
}
