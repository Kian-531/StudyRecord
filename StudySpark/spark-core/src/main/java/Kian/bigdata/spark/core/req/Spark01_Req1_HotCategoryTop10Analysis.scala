package Kian.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/26
 */
object Spark01_Req1_HotCategoryTop10Analysis {


  def main(args: Array[String]): Unit = {


    // TODO: Top10热门品类
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/Req/user_visit_action.txt")

    // 2. 统计品类点击数量: (品类ID, 点击数量)
    //    先过滤数据，只得到点击行为的数据
    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val data: Array[String] = action.split("_")
        data(6) != "-1"
      }
    )

    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val data: Array[String] = action.split("_")
        (data(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3. 统计品类下单数量: (品类ID, 下单数量)
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val data: Array[String] = action.split("_")
        data(8) != "null"
      }
    )

    //    orderID => 1,2,3
    //    [(1,1), (2,1), (3,1)]
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val data: Array[String] = action.split("_")
        val cid: String = data(8)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4. 统计品类支付数量: (品类ID, 支付数量)
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val data: Array[String] = action.split("_")
        data(10) != "null"
      }
    )

    //    orderID => 1,2,3
    //    [(1,1), (2,1), (3,1)]
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val data: Array[String] = action.split("_")
        val cid: String = data(10)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 5. 将品类进行排序，取前10名
    //    排序顺序: 点击数量排序，下单数量排序，支付数量排序
    //    scala元组排序: 先比较第一个，在比较第二个，在比较第三个
    //    2，3，4的数据转换为(品类ID,(点击数量，下单数量，支付数量))
    //
    // (join, zip, LeftOuterJoin), cogroup
    //
    // connect + group
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
    clickCountRDD.cogroup(orderCountRDD, payCountRDD)

    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {

        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }

        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }

    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)


    // 6. 将结果采集到数据台并打印出来
    resultRDD.foreach(println)


    sc.stop()


  }


}
