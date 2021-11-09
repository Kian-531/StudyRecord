package Kian.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * @author Kian
 * @date 2021/8/30
 */
object Spark03_SparkSQL_UDAF {


  def main(args: Array[String]): Unit = {


    // TODO 创建SparkSql环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    // TODO 自定义用户函数，将直接获取到的姓名前面加参数
    // ZhangSan => Name:ZhangSan

    spark.udf.register("avgAge", new myAvgUDAF)


    val df: DataFrame = spark.read.json("datas/SparkSql/user.json")
    df.createOrReplaceTempView("user")
    spark.sql("select avg(age) from user").show()

    spark.close()
    // TODO 关闭环境

  }

  /*
  自定义聚合函数类： 计算年龄的平均值
  1. 继承UserDefinedAggregateFunction
  2. 重写方法(8)
   */

  class myAvgUDAF extends UserDefinedAggregateFunction {
    // 输入数据的结构：In
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // 缓冲区数据的结构：Buffer
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    // 函数计算结果的类型：Out
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //      buffer(0) = 0L
      //      buffer(1) = 0L

      // 通过位置设置值
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 根据输入的值来更新缓冲区的值
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(0) + 1)
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
