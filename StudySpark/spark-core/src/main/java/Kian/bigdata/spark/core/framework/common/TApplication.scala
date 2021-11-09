package Kian.bigdata.spark.core.framework.common

import Kian.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/28
 */
trait TApplication {

  def start(Master: String = "local[*]", App: String = "Application")(op: => Unit) = {


    // TODO 建立和Spark框架的连接
    val sparkConf = new SparkConf().setMaster(Master).setAppName(App) //spark配置信息
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }

    // TODO 关闭连接
    sc.stop()
    EnvUtil.clear

  }
}
