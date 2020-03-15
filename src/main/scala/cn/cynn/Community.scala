package cn.cynn

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
class Community {
  var communityId = 0
  var communityIdGroup = new ArrayBuffer[VertexId]() // 存放该社区ID的节点数
  var innerEdgeNum = 0
}
