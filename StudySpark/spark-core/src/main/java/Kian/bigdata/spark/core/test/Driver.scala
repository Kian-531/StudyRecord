package Kian.bigdata.spark.core.test

import java.io.ObjectOutputStream
import java.net.Socket

/**
 * @author Kian
 * @date 2021/8/14
 */
object Driver {
  def main(args: Array[String]): Unit = {

    // 连接服务器
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val task = new Task

    // 给Executor1发送
    val out1 = client1.getOutputStream
    val objOut1 = new ObjectOutputStream(out1)

    val subTask1 = new SubTask
    subTask1.datas = task.dates.take(2)
    subTask1.logic = task.logic


    objOut1.writeObject(subTask1)

    objOut1.flush()
    objOut1.close()
    client1.close()

    // 给Executor2发送
    val out2 = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2)

    val subTask2 = new SubTask
    subTask2.datas = task.dates.takeRight(2)
    subTask2.logic = task.logic
    objOut2.writeObject(subTask2)

    objOut2.flush()
    objOut2.close()
    client2.close()

    println("客户端数据发送完毕")
  }
}
