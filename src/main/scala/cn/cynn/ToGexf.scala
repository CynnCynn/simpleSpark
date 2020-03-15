package cn.cynn

import org.apache.spark.graphx.{Graph, VertexId}

// 将分区结果写到gexf文件中
object ToGexf {
    def toGexf[VD,ED](g:Graph[VD,ED]) : String = {
      val string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<gexf xmlns=\"http://www.gexf.net/1.2draft\" version=\"1.2\">\n" +
        " <graph mode=\"static\" defaultedgetype=\"directed\">\n" +
        " <nodes>\n" +
        g.vertices.map(v => " <node id=\"" + v._1 + "\" label=\"" +
          v._2 + "\" />\n").collect.mkString +
        " </nodes>\n" +
        " <edges>\n" +
        g.edges.map(e => " <edge source=\"" + e.srcId +
          "\" target=\"" + e.dstId + "\" label=\"" + e.attr +
          "\" />\n").collect.mkString +
        " </edges>\n" +
        " </graph>\n" +
        "</gexf>"
      return string
    }
  def toVertex[VD,ED](g: Graph[(VertexId, VertexId), VertexId]) : String = {
    val string = g.vertices.map(v => v._1 + " " + v._2._1 + " " + v._2._2+"\n").collect.mkString
    return string
  }
  def toEdge[VD,ED](g: Graph[(VertexId, VertexId), VertexId]) : String = {
    val string = g.edges.map(e => e.srcId + " " + e.dstId + " " + e.attr+"\n").collect.mkString
    return string
  }
}
