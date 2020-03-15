package cn.cynn

import java.io.{FileWriter, PrintWriter}

import scala.io.Source

// 网络分区
object SplitFile {
  def main(args: Array[String]): Unit = {
    val path: String = "D:\\study\\课件\\毕业设计\\毕业论文数据\\facebook_combined\\"
    val fileName = path + "facebook_combined - 副本.txt"
    val source = Source.fromFile(fileName)
    val lines = source.getLines
    var outputFile = 0

    for(line <- lines){
      val a = line.split(" ")
      outputFile = a(0).toInt/100
      val out = new FileWriter(path+ "Facebook_split\\" + outputFile + ".txt",true)
      out.write(line+"\n")
//      println(line)
      out.close()
    }

    source.close;//记得要关闭source
  }
}
