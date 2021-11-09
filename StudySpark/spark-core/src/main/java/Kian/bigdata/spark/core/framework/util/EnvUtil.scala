package Kian.bigdata.spark.core.framework.util

import org.apache.spark.SparkContext

/**
 * @author Kian
 * @date 2021/8/28
 */
object EnvUtil {
  private val scLocal = new ThreadLocal[SparkContext]

  def put(sc: SparkContext) = {
    scLocal.set(sc)
  }

  def take(): SparkContext = {
    scLocal.get()
  }

  def clear = {
    scLocal.remove()
  }

}
