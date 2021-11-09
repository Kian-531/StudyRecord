package Kian.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._


/**
 * @author Kian
 * @date 2021/8/30
 */
object Spark04_SparkSQL_JDBC {


  def main(args: Array[String]): Unit = {


    // TODO 创建SparkSql环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    // 读取MySQL的数据
    // 配置MySQL
    val df: DataFrame = spark.read
      .format("JDBC")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql?serverTimezone=GMT")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "user")
      .load()
    df.show()

    // 保存数据
    df.write
      .format("JDBC")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql?serverTimezone=GMT")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()


    // TODO 关闭环境
    spark.close()

  }


}
