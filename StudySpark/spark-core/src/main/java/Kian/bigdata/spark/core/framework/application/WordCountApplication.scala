package Kian.bigdata.spark.core.framework.application

import Kian.bigdata.spark.core.framework.common.TApplication
import Kian.bigdata.spark.core.framework.controller.WordCountController


/**
 * @author Kian
 * @date 2021/8/28
 */
object WordCountApplication extends App with TApplication {

  // 启动应用程序
  start() {
    val controller = new WordCountController
    controller.dispatch
  }

}
