package Kian.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
 * @author Kian
 * @date 2021/8/30
 */
object Spark02_SparkSQL_UDF {


  def main(args: Array[String]): Unit = {


    // TODO 创建SparkSql环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._


    // TODO 自定义用户函数，将直接获取到的姓名前面加参数
    // ZhangSan => Name:ZhangSan

    spark.udf.register("preName", (name: String) => {
      "Name:" + name
    })


    val df: DataFrame = spark.read.json("datas/SparkSql/user.json")
    df.createOrReplaceTempView("user")
    spark.sql("select age,preName(username) from user").show()

    spark.close()
    // TODO 关闭环境

  }

}
