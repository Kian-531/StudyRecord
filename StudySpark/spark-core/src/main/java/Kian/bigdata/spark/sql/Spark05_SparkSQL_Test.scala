package Kian.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


/**
 * @author Kian
 * @date 2021/8/30
 */
object Spark05_SparkSQL_Test {


  def main(args: Array[String]): Unit = {


    // TODO 创建SparkSql环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .config("Spark.sql.warehouse.dir", "hdfs://192.168.91.100:9000/hive/warehouse")
      .getOrCreate()



    // TODO 准备数据
    spark.sql("use user_test")

    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |  row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/SparkSqlTest/user_visit_action.txt' into table user_test.user_visit_action
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |  row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/SparkSqlTest/product_info.txt' into table user_test.product_info
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,   `area` string)
        |row format delimited fields terminated by '\t'
        |
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'datas/SparkSqlTest/city_info.txt' into table user_test.city_info
        |""".stripMargin)

    spark.sql("select * from city_info").show()

    // TODO 关闭环境
    spark.close()

  }


}
