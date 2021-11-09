package Kian.bigdata.spark.core.framework.controller

import Kian.bigdata.spark.core.framework.common.TController
import Kian.bigdata.spark.core.framework.service.WordCountService

/**
 * @author Kian
 * @date 2021/8/28
 *       对数据的控制调度(调用Service层的逻辑方法)
 */
class WordCountController extends TController {
  private val wordCountService = new WordCountService

  // 调度
  def dispatch = {
    // TODO 执行业务操作
    val array: Array[(String, Int)] = wordCountService.dataAnalysis
    array.foreach(println)
  }
}
