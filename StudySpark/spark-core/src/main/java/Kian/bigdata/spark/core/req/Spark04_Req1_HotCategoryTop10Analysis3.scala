package Kian.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Kian
 * @date 2021/8/26
 */
object Spark04_Req1_HotCategoryTop10Analysis3 {


  def main(args: Array[String]): Unit = {


    // TODO: Top10热门品类
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // Q: 存在大量的shuffle操作(reduceByKey)
    // reduceByKey 聚合算子，Spark会提供优化缓存

    // 准备累加器
    val acc = new HotCategoryAccumulator
    sc.register(acc, "HotCategory")


    // 1. 读取原始日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/Req/user_visit_action.txt")
    actionRDD.cache()

    // 2. 将数据结构转换
    val flatRDD = actionRDD.foreach(
      action => {
        val data: Array[String] = action.split("_")
        if (data(6) != "-1") {
          // 点击的场合
          acc.add(data(6), "click")
        } else if (data(8) != "null") {
          // 下单的场合
          val ids: Array[String] = data(8).split(",")
          ids.foreach(
            id => {
              acc.add(id, "order")
            }
          )
        } else if (data(10) != "null") {
          // 支付的场合
          val ids: Array[String] = data(10).split(",")
          ids.foreach(
            id => {
              acc.add(id, "pay")
            }
          )
        }
      }
    )

    val accVal: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)

    val sort: List[HotCategory] = categories.toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )

    val resultRDD: List[HotCategory] = sort.take(10)

    // 5. 将结果采集到数据台并打印出来
    resultRDD.foreach(println)


    sc.stop()


  }

  // 定义一个数据类型作为累加器OUT类型
  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)

  /**
   * 自定义累加器
   * 1. 继承AccumulatorV2 定义泛型
   * IN : (品类ID, 行为类型)
   * OUT : mutable.Map[String,HotCategory]
   *
   * 2. 重写方法(6)
   */
  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
    private val hcMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val actionType: String = v._2

      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))

      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {

      val map1 = this.hcMap
      val map2 = other.value

      map2.foreach {
        case (cid, hc) => {
          val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          category.clickCnt += hc.clickCnt
          category.orderCnt += hc.orderCnt
          category.payCnt += hc.payCnt
          map1.update(cid, category)
        }
      }
    }

    override def value: mutable.Map[String, HotCategory] = {
      hcMap
    }
  }

}
