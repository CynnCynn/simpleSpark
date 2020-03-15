package cn.cynn


import java.util.Date

import cn.cynn.FunUtil.deleteOutPutPath
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import cn.cynn.KddFunc.compute
object Kdd {
  def main(args: Array[String]): Unit = {
//    val start_time = new Date().getTime
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel(Level.WARN.toString)

    val path: String = "D:\\study\\课件\\毕业设计\\毕业论文数据\\facebook_combined\\Facebook_split\\"

//    网络分区
    compute(path, "6", sc)

    spark.stop()
  }


  def findVertexProperty(graph: Graph[VertexId, Long], id: VertexId): VertexId = {
    for (a <- graph.vertices.collect()) {
      if (a._1 == id) {
        return a._2
      }
    }
    0L
  }

}
