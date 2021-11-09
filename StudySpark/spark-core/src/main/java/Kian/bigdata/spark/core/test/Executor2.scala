package Kian.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @author Kian
 * @date 2021/8/14
 */
object Executor2 {


  def main(args: Array[String]): Unit = {

    // 创建一个服务器
    val server = new ServerSocket(8888)
    println("loading...")


    // 等待客户端的连接
    val client = server.accept()
    val in = client.getInputStream
    val objIn = new ObjectInputStream(in)


    val task = objIn.readObject().asInstanceOf[SubTask]
    val ints = task.compute()

    println(s"8888节点计算的结果为: $ints")

    objIn.close()
    client.close()
    server.close()
  }
}
