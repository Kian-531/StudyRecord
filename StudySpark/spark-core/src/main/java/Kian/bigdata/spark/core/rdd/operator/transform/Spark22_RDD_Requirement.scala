package Kian.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/16
 */
object Spark22_RDD_Requirement {


  /*
    agent.log: 时间戳，省份，城市，用户，广告， 中间字段使用空格分割
    需求描述: 统计出每一个省份每个广告被点击数量的Top3
  */

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo")
    val sc = new SparkContext(sparkConf)

    // TODO 案例实操
    // 1. 获取元数据，时间戳，省份，城市，用户，广告
    val dataRDD: RDD[String] = sc.textFile("datas/agent.log")

    // 2. 数据转换
    // 时间戳，省份，城市，用户，广告
    // =>
    // ((省份, 广告), 1)
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      datas => {
        val data = datas.split(" ")
        ((data(1), data(4)), 1)
      }
    )

    // 3. 按照key做分组聚合
    // ((省份, 广告), 1) => ((省份, 广告), sum)
    // val groupRDD: RDD[((String, String), Int)] = mapRDD.groupByKey().map(t => (t._1, t._2.size))
    val groupRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    // 4. 数据转换
    // ((省份, 广告), sum) => (省份, (广告, sum))
    // val mapRDD2: RDD[(String, (String, Int))] = groupRDD.map(t => (t._1._1, (t._1._2, t._2)))
    val mapRDD2: RDD[(String, (String, Int))] = groupRDD.map {
      case ((prv, ad), sum) => {
        (prv, (ad, sum))
      }
    }

    // 5. 根据省份进行分组
    // (省份, 【(广告A, sumA), (广告B, sumB)】)
    val groupRDD2: RDD[(String, Iterable[(String, Int)])] = mapRDD2.groupByKey()

    // 6. 分组后的数据组内按照广告点击量排序(降序)取Top3
    //    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD2.map(t => {
    //      val ad = t._2.toList
    //      (t._1, ad.sortWith(_._2 > _._2).take(3))
    //    })
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD2.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      })


    // 收集打印在控制台
    resultRDD.collect().foreach(println)


    sc.stop()
  }
}
