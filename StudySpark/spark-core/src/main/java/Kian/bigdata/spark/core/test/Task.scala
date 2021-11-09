package Kian.bigdata.spark.core.test

/**
 * @author Kian
 * @date 2021/8/14
 */
class Task extends Serializable {

  val dates = List(1, 2, 3, 4)

  val logic: Int => Int = _ * 2


}
