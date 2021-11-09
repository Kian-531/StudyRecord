package Kian.bigdata.spark.core.framework.common

import Kian.bigdata.spark.core.framework.util.EnvUtil

/**
 * @author Kian
 * @date 2021/8/28
 */
trait TDao {
  def readFile(path: String) = {
    EnvUtil.take().textFile(path)
  }
}
