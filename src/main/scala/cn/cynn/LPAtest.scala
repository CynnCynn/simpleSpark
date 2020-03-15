package cn.cynn

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object LPAtest {
  /**
    * 在社交网络中用lpa发现社区
    * 网络中的每个节点都有一个初始的社区标识，每次节点发送自己的社区标识到自己所有的邻居节点并且更新所有节点的模态社区
    * lpa是一个标准的社区发现算法，它的计算是廉价的，虽然他不能保证一定会收敛，但是它可以使用某些规则结束其迭代
    *
    * @tparam ED 边属性的类型
    * @param graph    需要计算社区的图
    * @param maxSteps 由于是静态实现，设置最大的迭代次数
    * @return 返回的结果在图的点属性中包含社区的标识 */
//  def main[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
//    //初始化每个定点的社区表示为当前结点的id值
//    val lpaGraph = graph.mapVertices { case (vid, _) => vid }
//
//    //定义消息的发送函数，将顶点的社区标识发送到相邻顶点
//    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
//      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
//    }
//
//    //顶点的消息聚合函数 将每个节点消息聚合，做累加；例如一个定点出现了两次，id->2
//    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
//    : Map[VertexId, Long] = {
//      (count1.keySet ++ count2.keySet).map { i =>
//        val count1Val = count1.getOrElse(i, 0L)
//        val count2Val = count2.getOrElse(i, 0L)
//        i -> (count1Val + count2Val)
//      }.toMap
//    }
//
//    //该函数用于在完成一次迭代的时候，将第一次的结果和原图做关联
//    //如果当前结点的message为空，则将该节点的社区标识设置为当前结点的id，如果不为空，在根据其他节点在出现的次数求最大值，（可以把他看成是一种规则，slpa是基于重叠社区的发现，slpa则使用出现次数的阈值等规则）
//    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
//      if (message.isEmpty) attr else message.maxBy(_._2)._1
//    }
//
//    //    val initialMessage = MapVertexId
//    //      , Long
//    //    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
//    //      vprog = vertexProgram,
//    //      sendMsg = sendMessage,
//    //      mergeMsg = mergeMessage)
//
//
//    val spark = SparkSession
//      .builder
//      .appName(s"${
//        this.getClass.getSimpleName
//      }").master("local[*]")
//      .getOrCreate()
//    val sc = spark.sparkContext
//    sc.setLogLevel(Level.WARN.toString)
//    val rdd = sc.makeRDD(Array("1 2", "1 3", "2 4", "3 4", "3 5", "4 5", "5 6", "6 7",
//      "6 9", "7 11", "7 8", "9 8", "9 13", "8 10", "10 13",
//      "13 12", "10 11", "11 12"
//    ))
//    val edge = rdd.map(line => {
//      val pair = line.split(" ")
//      Edge(pair(0).toLong, pair(1).toLong, 1L)
//    })
//    val graph = Graph.fromEdges(edge, 0)
//    val label = LabelPropagation.run(graph, 5)
//
//    label.vertices.foreach(println(_))
//
//  }
}
