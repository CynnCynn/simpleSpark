package cn.cynn

import java.util.Date

import cn.cynn.FunUtil.deleteOutPutPath
import org.apache.log4j.Level
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MergeGraph {
  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel(Level.WARN.toString)

    val path: String = "D:\\study\\课件\\毕业设计\\毕业论文数据\\facebook_combined\\"
    val records: RDD[String] = sc.textFile(path + "facebook_combined - 副本.txt") //
    val followers = records.map { case x => val fields = x.split(" ")
      Edge(fields(0).toLong, fields(1).toLong, 1L)
    }
    val graph = Graph.fromEdges(followers, 0L) // 默认值为0，表示未分社区
    val vertex: VertexRDD[VertexId] = graph.vertices

    val edge_filter = graph.edges.filter(a =>
      a.srcId != a.dstId)
    graph.unpersist()
    var graph_filter: Graph[VertexId, Long] = Graph(vertex, edge_filter)
    val end_time = new Date().getTime
    println(end_time - start_time)
    sc.hadoopConfiguration.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true)
    deleteOutPutPath(sc, path + "need_pay")
    //    deleteOutPutPath(sc, path + "neighbourhood")
    sc.parallelize(graph_filter.vertices.collect()).repartition(1).saveAsTextFile(path + "need_pay")
    val pw = new java.io.PrintWriter(path + "myGraph.gexf")
    pw.write(ToGexf.toGexf(graph_filter))
    pw.close
    //    sc.parallelize(neighbourhood.collect).repartition(1).saveAsTextFile(path + "neighbourhood")
    spark.stop()
  }
}