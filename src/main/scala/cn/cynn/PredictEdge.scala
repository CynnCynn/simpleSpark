package cn.cynn

import java.io.FileWriter

import cn.cynn.FunUtil.deleteOutPutPath
import org.apache.log4j.Level
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object PredictEdge {
  def main(args: Array[String]): Unit = {
    //    val start_time = new Date().getTime
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel(Level.WARN.toString)

    val partition = 2


    val path: String = "D:\\study\\课件\\毕业设计\\毕业论文数据\\facebook_combined\\Facebook_split\\"
//    划分训练集和测试集
    val fileName = path + "6subCommEdge"+partition+".txt"
    val source = Source.fromFile(fileName)
    val lines = source.getLines
    deleteOutPutPath(sc, path + "6subCommEdge"+partition+"_train.txt")
    deleteOutPutPath(sc, path + "6subCommEdge"+partition+"_test.txt")

//    划分训练集和测试集
    for(line <- lines){
      val rand = scala.util.Random.nextInt(100).toDouble/100
      if(rand < 0.9)
        {
      val outTrain = new FileWriter(path+ "6subCommEdge"+partition+"_train.txt",true)
          outTrain.write(line+"\n")
      //      println(line)
          outTrain.close()
    }
      else{
        val outTest = new FileWriter(path+ "6subCommEdge"+partition+"_test.txt",true)
        outTest.write(line+"\n")
        //      println(line)
        outTest.close()
      }
    }

    source.close;//记得要关闭source

//    链路预测
    val edgeRecords: RDD[String] = sc.textFile(path + "6subCommEdge"+partition+"_train.txt") //
    val edges = edgeRecords.map { x =>
      val fields = x.split(" ")
      Edge(fields(0).toLong, fields(1).toLong, fields(2).toLong)
    }
    val vertexRecords: RDD[String] = sc.textFile(path + "6subCommVertex"+ partition+ ".txt") //
    val vertexs = vertexRecords.map { line =>
      val lines = line.split(" ")
      (lines(0).toLong, 0L)
    }
    val gStart = Graph(vertexs, edges)
    val subDegree = gStart.degrees.map(a => (a._1, a._2))
    val g = gStart.joinVertices(subDegree) {
      case (id, _, subDegree) => subDegree
    }
    val subNeigh: VertexRDD[Array[(VertexId, VertexId)]] = g.collectNeighbors(EdgeDirection.Either)
    var vertexE = new ArrayBuffer[(VertexId, Long)]()

    for (vertex <- g.vertices.collect()) {
      val array: Array[VertexId] = subNeigh.lookup(vertex._1).head.map(a => a._1)
      if (array.length > 1) {
        val neigSubGraph = {
          g.subgraph(vpred = (id, _) => array.contains(id))
        }

        vertexE ++= Array((vertex._1, neigSubGraph.numEdges))
      }
      else {
        vertexE ++= Array((vertex._1, 0L))
      }
    }
    val graph: Graph[(VertexId, VertexId), VertexId] = g.outerJoinVertices(sc.parallelize(vertexE))((_, old, deg) => (old, deg.getOrElse(0)))
    val graphSubNeigh = graph.collectNeighbors(EdgeDirection.Either)


    var potentialEdge: ArrayBuffer[(VertexId, VertexId, Double)] = new ArrayBuffer[(VertexId, VertexId, Double)]() //(srcId, dstId, Probability)
//    最大最小归一化
    var max = 0.0
    var min = 0.0
    //        两个节点之间连接的概率
    for (vertex1 <- graph.vertices.collect()) {
      for (vertex2 <- graph.vertices.collect()) {
        if (vertex1._1 < vertex2._1) {
          //              看他们是否已经存在边
          val vertex1Neigh = graphSubNeigh.lookup(vertex1._1).head
          val vertex1NeighArray = vertex1Neigh.map(a => a._1)
          var cluster = 0.0 // 两个节点连成边的概率
          if (!vertex1NeighArray.contains(vertex2._1)) {
            val vertex2Neigh = graphSubNeigh.lookup(vertex2._1).head
            val coNeig: Array[(VertexId, (VertexId, VertexId))] = vertex2Neigh.union(vertex1Neigh) // 共邻节点
            for (a <- coNeig) {
              //                  val neighVertex = findVertex(graph, a)
              if (a._2._1 > 1) {
                cluster += 2 * a._2._2.toDouble / (a._2._1 * a._2._1 * (a._2._1 - 1)) + 1.0 / a._2._1
                //                    cluster += 2 * neighVertex._2._2.toDouble / (neighVertex._2._1 * neighVertex._2._1 * (neighVertex._2._1 - 1)) + 1.0 / neighVertex._2._1
              }
            }
          }

          if (cluster > 0) {
            if(max < cluster){
              max = cluster
            }
            if(min > cluster)
              {
                min = cluster
              }
            potentialEdge ++= Array((vertex1._1, vertex2._1, cluster))
//            println(vertex1._1 + " " + vertex2._1 + " " + cluster)
          }
        }

      }
    }

    val realEdges: RDD[Edge[VertexId]] = sc.textFile(path + "6subCommEdge"+partition+"_test.txt").map { x =>
      val fields = x.split(" ")
      Edge(fields(0).toLong, fields(1).toLong, fields(2).toLong)
    }

    for(a<-realEdges){
      println(a.srcId+" "+a.dstId + " " + (findEdegProbability(a.srcId, a.dstId,potentialEdge)-min)/(max-min))
    }

//    将生成边的概率写入文件
    val pw = new java.io.PrintWriter(path + "ROC_result.txt")
    for(a <- potentialEdge){
      if(isRealEdge(a._1, a._2, realEdges))
        pw.write(1 + " " + (a._3-min)/(max-min) + "\n")
      else{
        pw.write(0 + " " + (a._3-min)/(max-min) + "\n")
      }
    }
    pw.close()


    //    val graph = GraphLoader.edgeListFile(sc, path + "SubCommEdge_2\\part-00000").cache()
    sc.stop()
  }

  def findEdegProbability(srcId:VertexId, dstId:VertexId, potentialEdge: ArrayBuffer[(VertexId, VertexId, Double)]): Double ={
    for(a<-potentialEdge){
      if(a._1 == srcId && a._2 == dstId)
        return a._3
    }
    0.0
  }

  def isRealEdge(srcId:VertexId, dstId:VertexId, realEdges: RDD[Edge[VertexId]]): Boolean =
  {
    for(a<-realEdges.collect()){
      if(srcId == a.srcId && dstId == a.dstId){
        return true
      }
    }
    false
  }
}
