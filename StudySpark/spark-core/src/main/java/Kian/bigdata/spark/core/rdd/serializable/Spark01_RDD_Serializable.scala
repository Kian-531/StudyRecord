package Kian.bigdata.spark.core.rdd.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Kian
 * @date 2021/8/19
 */
object Spark01_RDD_Serializable {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Serializable")
    // 配置Kryo序列化
    //      .set("spark.serializer",
    //        "org.apache.spark.serializer.KryoSerializer")
    // 注册需要使用 kryo 序列化的自定义类
    //      .registerKryoClasses(Array(classOf[Search]))

    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array(
      "hello world", "hello spark", "spark", "hadoop"
    ), 2)

    val search = new Search("s")
    search.getMatch1(rdd).collect().foreach(println)


    sc.stop()
  }


  // 查询对象
  // 类的构造参数是类的属性，构造参数需要进行闭包检测，就等同于类进行闭包检测
  // 解决思路：1.给类序列化
  //         2.将类的属性的值赋值给方法内部属性
  class Search(query: String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }

    // 函数序列化案例
    def getMatch1(rdd: RDD[String]): RDD[String] = {
      rdd.filter(isMatch)
    }

    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      //      val s = query
      //      rdd.filter(x => x.contains(s))
      rdd.filter(x => x.contains(query))
    }
  }

}


