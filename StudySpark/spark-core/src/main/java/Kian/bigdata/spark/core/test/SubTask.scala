package Kian.bigdata.spark.core.test

/**
 * @author Kian
 * @date 2021/8/14
 */
class SubTask extends Serializable {

  var datas: List[Int] = _
  var logic: Int => Int = _

  // 计算
  def compute(): List[Int] = {
    datas.map(logic)
  }

}
