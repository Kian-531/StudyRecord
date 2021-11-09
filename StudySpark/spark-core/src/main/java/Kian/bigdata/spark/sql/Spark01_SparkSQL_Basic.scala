package Kian.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


/**
 * @author Kian
 * @date 2021/8/30
 */
object Spark01_SparkSQL_Basic {


  def main(args: Array[String]): Unit = {


    // TODO 创建SparkSql环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._


    // TODO 执行逻辑
    // DataFrame
    //    val df: DataFrame = spark.read.json("datas/SparkSql/user.json")
    //    df.show()
    // DataFrame -> SQL
    //    df.createOrReplaceTempView("user")
    //    spark.sql("select * from user").show()
    //    spark.sql("select age,username from user").show()
    //    spark.sql("select avg(age) from user").show()

    // DataFrame -> DSL
    // DateFrame涉及到转换操作，需要引入转换规则

    //    df.select("age", "username").show()
    //    df.select($"age" + 1).show
    //    df.select('age + 1).show()


    // TODO DataSet
    // DataFrame就是特定泛型的DataSet
    // type DataFrame = Dataset[Row]
    //
    //    val seq = Seq(1, 2, 3, 4)
    //    val ds: Dataset[Int] = seq.toDS()
    //    ds.show()


    // RDD <=>DataFrame
    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("ZhangSan", 20), ("LiSi", 30), ("WangWu", 40)))
    val rddToDf: DataFrame = rdd.toDF("name", "age")
    rddToDf.show()

    val rdd1: RDD[Row] = rddToDf.rdd

    // DataFrame <=>DataSet
    val dfToDs: Dataset[User] = rddToDf.as[User]
    dfToDs.show()

    val dsToDf: DataFrame = dfToDs.toDF()

    // RDD <=>DataSet
    // RDD 转换到ds需要借用样例类，先将RDD中的数据转换为样例类的类型，然后转换为ds
    val rddToDs: Dataset[User] = rdd.map {
      case (name, age) => {
        User(name, age)
      }
    }.toDS()

    val dsToRDD: RDD[User] = rddToDs.rdd

    spark.close()
    // TODO 关闭环境

  }

  case class User(name: String, age: Int)

}
