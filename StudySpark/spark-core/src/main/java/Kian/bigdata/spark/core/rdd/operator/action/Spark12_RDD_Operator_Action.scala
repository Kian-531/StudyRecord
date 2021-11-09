package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark12_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 2), ("a", 2), ("c", 3), ("b", 6), ("a", 3)
    ), 2)

    // TODO - 行动算子 - save相关

    // 保存成Text文件
    rdd.saveAsTextFile("output")

    // 序列化成对象保存到文件
    rdd.saveAsObjectFile("output1")

    // 保存成sequenceFile文件(数据类型必须是key-value类型)
    rdd.saveAsSequenceFile("output2")


    sc.stop()

  }
}
