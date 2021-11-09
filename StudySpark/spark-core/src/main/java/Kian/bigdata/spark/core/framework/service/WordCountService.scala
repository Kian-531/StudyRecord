package Kian.bigdata.spark.core.framework.service

import Kian.bigdata.spark.core.framework.common.TService
import Kian.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * @author Kian
 * @date 2021/8/28
 *       业务逻辑代码(定义对数据的操作方法)
 */
class WordCountService extends TService {
  private val wordCountDao = new WordCountDao


  // 数据分析
  def dataAnalysis = {
    val lines: RDD[String] = wordCountDao.readFile("datas/word.txt")
    val words = lines.flatMap(_.split(" "))
    val wordGroup = words.groupBy(word => word)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    val array = wordToCount.collect()
    array
  }
}
