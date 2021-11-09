package Kian.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/18
 */
object Spark14_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Action")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      1, 2, 3, 4
    ), 2)

    // TODO - 行动算子 - foreach

    val user = new User()


    // Task not serializable
    // Kian.bigdata.spark.core.rdd.operator.action.Spark14_RDD_Operator_Action$User Serialization stack:
    // RDD外部的代码是在Driver端执行
    //     在Driver端创建的user
    // RDD内部的代码在Executor端执行
    //     在Executor端执行打印操作，但是Executor端需要访问user中的数据，user中的数据不在Executor端，需要序列化User


    // RDD算子中传递的函数是会包含闭包操作的，就会进行检测功能
    // 闭包检测
    rdd.foreach(
      num => {
        println(s"age = ${user.age + num}")
      }
    )

    sc.stop()


  }

  //  class User extends Serializable
  // 样例类在编译时会自动混入序列化的特质(实现可序列化接口)
  case class User() {
    var age: Int = 30
  }

}
