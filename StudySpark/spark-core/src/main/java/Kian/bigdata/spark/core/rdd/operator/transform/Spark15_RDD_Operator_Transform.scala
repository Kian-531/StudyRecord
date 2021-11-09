package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark15_RDD_Operator_Transform {


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)


    // TODO 算子 - key-value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("b", 4)))


    // reduceByKey:相同的key做value的聚合
    // reduceByKey中如果只有一个数据，是不会参与聚合操作的
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x, y) => {
      x + y
    })

    reduceRDD.collect().foreach(println)

    sc.stop()
  }
}
