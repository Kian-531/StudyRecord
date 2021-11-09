package Kian.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/26
 */
object Spark06_Req3_PageFlowAnalysis {


  def main(args: Array[String]): Unit = {


    // TODO: Top10热门品类

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)


    val actionRDD: RDD[String] = sc.textFile("datas/Req/user_visit_action.txt")

    val actionDataRDD = actionRDD.map(
      action => {
        val data: Array[String] = action.split("_")
        UserVisitAction(
          data(0),
          data(1).toLong,
          data(2),
          data(3).toLong,
          data(4),
          data(5),
          data(6).toLong,
          data(7).toLong,
          data(8),
          data(9),
          data(10),
          data(11),
          data(12).toLong,
        )
      }
    )

    actionDataRDD.cache()

    // TODO 对指定的页面连续跳转进行统计
    // 1-2,2-3,3-4,4-5,5-6,6-7
    // 1,  2,  3,  4,  5,  6
    // init:不包含最后一个数
    val ids = List[Long](1, 2, 3, 4, 5, 6, 7)
    val okIds: List[(Long, Long)] = ids.zip(ids.tail)

    // TODO 计算分母
    val pageIDToCountMap = actionDataRDD.filter(
      action => {
        ids.init.contains(action.page_id)
      }
    ).map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _).collect().toMap

    // TODO 计算分子
    // 根据Session分组
    val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = actionDataRDD.groupBy(_.session_id)

    // 根据时间排序(升序)
    val mvRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)

        // 【1,2,3,4】
        // 【1,2】,【2,3】,【3,4】
        // 【1-2,2-3,3-4】
        // Sliding: 滑窗

        // 【1,2,3,4】
        // 【2,3,4】
        // zip: 拉链
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)

        // 将不合法的页面跳转进行过滤
        pageFlowIds.filter(
          t => {
            okIds.contains(t)
          }
        ).map(
          t => {
            (t, 1)
          }
        )
      }
    )

    // ((1-2),1)
    val flatRDD: RDD[((Long, Long), Int)] = mvRDD.map(_._2).flatMap(list => list)

    // ((1-2),sum)
    val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)


    // TODO 计算单跳转换率
    // 分子除以分母

    dataRDD.foreach {
      case ((pageID1, pageID2), sum) => {
        val lon: Long = pageIDToCountMap.getOrElse(pageID1, 0L)

        println(s"${pageID1}-${pageID2}的单跳转换率为: ${sum.toDouble / lon}")
      }
    }

    sc.stop()


  }

  //用户访问动作表
  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的ID
                              session_id: String, //Session的ID
                              page_id: Long, //某个页面的ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的ID
                              click_product_id: Long, //某一个商品的ID
                              order_category_ids: String, //一次订单中所有品类的ID集合
                              order_product_ids: String, //一次订单中所有商品的ID集合
                              pay_category_ids: String, //一次支付中所有品类的ID集合
                              pay_product_ids: String, //一次支付中所有商品的ID集合
                              city_id: Long //城市 id
                            )

}
