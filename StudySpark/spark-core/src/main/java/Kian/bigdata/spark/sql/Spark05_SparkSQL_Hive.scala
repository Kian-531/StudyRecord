package Kian.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


/**
 * @author Kian
 * @date 2021/8/30
 */
object Spark05_SparkSQL_Hive {


  def main(args: Array[String]): Unit = {


    // TODO 创建SparkSql环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .config("Spark.sql.warehouse.dir", "hdfs://192.168.91.100:9000/hive/warehouse")
      .getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1.拷贝hive.site.xml文件到classpath下
    // 2.启用hive的支持( .enableHiveSupport() )
    // 3.增加对应的依赖关系(包含MySQL的驱动)

    spark.sql("show databases").show()

    // TODO 关闭环境
    spark.close()

  }


}
