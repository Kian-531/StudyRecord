package Kian.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Kian
 * @date 2021/8/24
 */
object Spark04_Acc_WordCount {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List(
      "hello", "spark", "hello", "scala"
    ))

    // 累加器 WordCount
    // 创建累加器对象
    val wcAcc = new myAcc

    // 向Spark注册累加器
    sc.register(wcAcc, "wordCountAcc")

    rdd.foreach(
      word => {
        // 数据的累加(使用累加器)
        wcAcc.add(word)
      }
    )

    println(wcAcc.value)
    // 获取累加器的值

    sc.stop()
  }


  /*
   自定义累加器 WordCount

      1. 继承AccumulatorV2, 定义泛型
      IN :要累加的数据类型
      OUT:输出的数据类型

      2. 重写方法
   */

  class myAcc extends AccumulatorV2[String, mutable.Map[String, Long]] {

    private val map: mutable.Map[String, Long] = mutable.Map()

    // 累加器是否为初始状态
    override def isZero: Boolean = map.isEmpty

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new myAcc

    // 重置累加器
    override def reset(): Unit = map.clear()

    // 向累加器中增加数据
    override def add(word: String): Unit = {
      // 查询map中是否存在相同的单词
      // 如果有相同的单词，那么单词的数量加1
      // 如果没有相同的单词，那么在map中增加这个单词
      map(word) = map.getOrElse(word, 0L) + 1L
    }

    // Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1 = this.map
      val map2 = other.value

      map2.foreach {
        case (word, count) => {
          val newCount: Long = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }


    // 返回累加器的结果
    override def value: mutable.Map[String, Long] = map


  }

}
