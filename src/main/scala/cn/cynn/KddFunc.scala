package cn.cynn

import java.util.Date

import cn.cynn.Kdd.findVertexProperty
import cn.cynn.FunUtil.deleteOutPutPath
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object KddFunc {

  def findVertexProperty(graph: Graph[VertexId, Long], id: VertexId): VertexId = {
    for (a <- graph.vertices.collect()) {
      if (a._1 == id) {
        return a._2
      }
    }
    0L
  }

  def findVertex(graph: Graph[(VertexId, VertexId), VertexId], id: VertexId): (VertexId, (VertexId, VertexId)) = {
    for (a: (VertexId, (VertexId, VertexId)) <- graph.vertices.collect()) {
      if (a._1 == id) {
        return a
      }
    }
    (0, (0, 0))
  }

  /**
    * 网络分区操作
    * @param path 文件路径
    * @param partition 文件名
    * @param sc SparkContext
    */
  def compute(path: String, partition: String, sc: SparkContext): Unit = {
    val start_time = new Date().getTime
    val records: RDD[String] = sc.textFile(path + partition + ".txt") //
    val followers = records.map { case x => val fields = x.split(" ")
      Edge(fields(0).toLong, fields(1).toLong, 1L)
    }
    val graph = Graph.fromEdges(followers, 0L) // 默认值为0，表示未分社区


    val communityId = 0
    //    过滤源节点与目标节点相同的边
    val vertex: VertexRDD[VertexId] = graph.vertices
    //
    val edge_filter = graph.edges.filter(a =>
      a.srcId != a.dstId)
    graph.unpersist()
    var graph_filter: Graph[VertexId, Long] = Graph(vertex, edge_filter)
    val numEdge = graph_filter.numEdges
    var indegrees_graph: RDD[(PartitionID, VertexId)] = graph_filter.degrees.map(a => (a._2, a._1)).sortByKey(false, 1)
    var indegrees_graph_sort: Array[(VertexId, PartitionID)] = indegrees_graph.collect.map(a => (a._2, a._1))

    //    val g: Graph[(PartitionID, VertexId), VertexId] = graph_filter.outerJoinVertices(graph.degrees)((_, old, deg) => (deg.getOrElse(0), old))
    //           .subgraph(vpred = (_, a) => a._1 > 1)
    //          //去掉大节点
    //    val newg: Graph[VertexId, VertexId] = g.mapVertices((_, attr)=>attr._2)
    //    graph_filter = newg
    //    newg.unpersist()
    //    indegrees_graph = graph_filter.degrees.map(a => (a._2, a._1)).sortByKey(false, 1)
    //    indegrees_graph_sort = indegrees_graph.collect.map(a => (a._2, a._1))
    println("节点个数: " + graph_filter.numVertices)

    //    val g = graph.joinVertices(graph.degrees){case(id, _) => graph.degrees}
    //
    //    val vertex = g.vertices.filter{case(id,(_, degree))=>degree>1}
    //
    //    val graph_filter = Graph[VertexId, Long] = Graph(vertex, edge_filter)

    //    合并社区
    val edgeNum = graph_filter.numEdges
    val neighbourhood: VertexRDD[Array[(VertexId, VertexId)]] = graph_filter.collectNeighbors(EdgeDirection.Either)

    val mapCommunity = scala.collection.mutable.Map[Long, ArrayBuffer[VertexId]]()

    var commId = 0L

    var vertexNum = 0.8 * graph_filter.numVertices
    //(graph_filter.numEdges*2).toDouble/graph_filter.numVertices // 平均节点度

    var averDegree = numEdge * 2 / graph_filter.numVertices
    var step = 5 // 平均节点度的步进 averDegree - 1
    println("平均节点度: " + averDegree)
    averDegree -= step
    if (averDegree < 2)
      averDegree = 2

    println("处理分区: " + partition)
    println("平均节点度: " + averDegree)
    var minDegree = 0
    //    将节点度较高的顶点归为一类
    breakable {
      for (a <- indegrees_graph_sort) {

        if (a._2 <= averDegree) {
          //          print("Hello")
          minDegree = a._2
          break()
        }
        if (vertexNum < 0) {
          minDegree = a._2
          //          print("hello")
          break()
        }
        //        该节点未分区
        if (findVertexProperty(graph_filter, a._1) == 0L) {
          commId += 1
          println(commId + " " + vertexNum)
          //        更新该节点社区信息
          val array = new ArrayBuffer[VertexId]()
          var updateV = new ArrayBuffer[(VertexId, Long)]()
          var newCommId = 0L
          var neighMax = 0L
          var neighCommSizeMax = 0
          updateV ++= Array((a._1, commId))
          var vertexNeighCommId = new ArrayBuffer[VertexId]()
          vertexNum -= 1
          array += a._1
          //          更新其邻居节点社区分类
          for (neighbour <- neighbourhood.lookup(a._1)) {
            breakable {
              for (neigh <- neighbour) {
                newCommId = findVertexProperty(graph_filter, neigh._1) // 查看邻居节点的分区
                if (newCommId == 0L &&
                  neighbourhood.lookup(neigh._1).head.length < averDegree) { // 邻居节点未分社区且平均节点度较小
                  vertexNum -= 1
                  updateV ++= Array((neigh._1, commId))
                  array += neigh._1
                }
                // 邻居节点已分区
                else if (newCommId != 0L) {
                  //                  统计其邻居节点的分区信息
                  vertexNeighCommId.append(newCommId)
                }
              }
            }
          }
          newCommId = 0
          //          如果要和其中一个大节点分区
          //          相邻节点已经超一半分到同一个社区
          if (vertexNeighCommId.length * 2 > neighbourhood.lookup(a._1).head.length) {
            var neighComID = 1

            while (neighComID < commId) {
              var neighCommSize = 0
              for (a <- vertexNeighCommId) {
                if (a == neighComID)
                  neighCommSize += 1
              }
              if (neighCommSizeMax < neighCommSize) {
                neighCommSizeMax = neighCommSize
                neighMax = neighComID
              }
              neighComID += 1
            }
            commId -= 1
            newCommId = neighMax
            // 和邻居节点分为一个社区
            //                  array += neigh._1
            updateV.clear()
            for (a <- array) {
              updateV ++= Array((a, neighMax))
            }
          }
          val updateVertex: RDD[(VertexId, VertexId)] = sc.parallelize(updateV)
          val graph_tmp = graph_filter.joinVertices(updateVertex) {
            case (_, _, updateVertex) => updateVertex
          }
          updateVertex.unpersist()
          updateV.clear()
          graph_filter = graph_tmp
          graph_tmp.unpersist()

          if (newCommId != 0) {
            array ++= mapCommunity(newCommId)
            mapCommunity += (newCommId -> array)
          }
          else {
            mapCommunity += (commId -> array)
          }
        }
        averDegree += step

      }

    }

    commId += 1
    //    社团发现算法

    for (a <- indegrees_graph_sort) {
      //      找到该节点对应的社区编号
      //        if(a._2<=3)
      //        break()
      if (a._2 <= minDegree && a._2 > 1 &&
        findVertexProperty(graph_filter, a._1) == 0L) { //未分社区
        var deltaQMax: Double = -1
        var commIdMax: Long = 0L
        var neighbourMax: VertexId = 0L
        for (neighbour <- neighbourhood.lookup(a._1)) { // 查看他的邻居节点
          breakable {
            for (neigh <- neighbour) {
              if (findVertexProperty(graph_filter, neigh._1) == 0L) { // 邻居节点未分社区
                val deltaQ = 2 * (1.0 / (2 * numEdge) - neighbourhood.lookup(neigh._1).head.length.toDouble / (2 * numEdge) * (neighbour.length.toDouble / (2 * numEdge)))
                if (deltaQMax < deltaQ) {
                  deltaQMax = deltaQ // 计算Q值
                  commIdMax = commId
                  neighbourMax = neigh._1
                }
              }
              else { // 邻居已分区
                val arrayBuffer: ArrayBuffer[VertexId] = mapCommunity(findVertexProperty(graph_filter, neigh._1))
                //                val subGraph = {
                //                  graph_filter.subgraph(vpred = (id, communityId) => arrayBuffer.contains(id))
                //                }
                //                val subGraph1 = {
                //                  graph_filter.subgraph(vpred = (id, communityId) => arrayBuffer.contains(id) || id == neigh._1)
                //                }
                var numE = 0
                for (neighneigh <- neighbourhood.lookup(a._1)) {
                  for (neig <- neighneigh) {
                    if (arrayBuffer.contains(neig._1)) {
                      numE += 1
                    }
                  }
                }
                //                arrayBuffer.clear()
                val deltaQ = 2 * (numE.toDouble / (2 * numEdge) - neighbourhood.lookup(neigh._1).head.length.toDouble / (2 * numEdge) * (neighbour.length.toDouble / (2 * numEdge))) // 计算Q值
                if (deltaQMax <= deltaQ) {
                  deltaQMax = deltaQ
                  commIdMax = findVertexProperty(graph_filter, neigh._1)
                }
                if (numE / neighbour.length > 0)
                  break()
              }
            }
          }
          //          和邻居节点分到同一个新社区
          if (commIdMax == commId) { // 新社区
            commId += 1
            val array = new ArrayBuffer[VertexId]()
            array += a._1
            array += neighbourMax


            //            var graph_filter_sort_tmp = indegrees_graph_sort.filter { case (vertexId, _) => !array.contains(vertexId) }
            //            indegrees_graph_sort = graph_filter_sort_tmp
            //            graph_filter_sort_tmp = null

            mapCommunity += (commIdMax -> array)
            //            更新该节点和邻居节点
            val updateVertex: RDD[(VertexId, VertexId)] = sc.parallelize(Array((a._1, commIdMax),
              (neighbourMax, commIdMax)))
            //

            val graph_tmp = graph_filter.joinVertices(updateVertex) {
              case (_, _, updateVertex) => updateVertex
            }
            graph_filter = graph_tmp
            graph_tmp.unpersist()
          }
          else {
            val array = new ArrayBuffer[VertexId]
            array ++= mapCommunity(commIdMax)
            array += a._1
            mapCommunity += (commIdMax -> array)

            //            更新该节点以及他的邻居节点

            val updateVertex: RDD[(VertexId, VertexId)] = sc.parallelize(Array((a._1, commIdMax)))
            val graph_tmp = graph_filter.joinVertices(updateVertex) {
              case (_, _, updateVertex) => updateVertex
            }
            graph_filter = graph_tmp
            graph_tmp.unpersist()
          }

          //          println(a._1 + "(" + a._2 + ")" + ":" + commIdMax + " ")
        }
      }
      else if (a._2 == 1 && findVertexProperty(graph_filter, a._1) == 0L) {
        //          要么跟邻居节点一个社区，要么自己一个社区
        for (neighbour <- neighbourhood.lookup(a._1)) { // 查看他的邻居节点
          //          计算Q值
          for (neigh <- neighbour) {
            val commId = findVertexProperty(graph_filter, neigh._1)
            if (commId != 0) { // 邻居节点已分社区
              val graph_tmp = graph_filter.joinVertices(sc.parallelize(Array((a._1, commId)))) {
                case (_, _, updateVertex) => updateVertex
              }
              graph_filter = graph_tmp
              graph_tmp.unpersist()
              val array = new ArrayBuffer[VertexId]
              array ++= mapCommunity(commId)
              array += a._1
              mapCommunity += (commId -> array)


              //              println(a._1 + "(" + a._2 + ")" + ":" + commId + " ")
            }
          }
        }
      }

    }

    //    计算Q值
    var modularityQ: Double = 0.0
    for (comm: (VertexId, ArrayBuffer[VertexId]) <- mapCommunity) {
      //      计算ai的平方
      val subGraph = {
        graph_filter.subgraph(vpred = (id, communityId) => comm._2.contains(id))
      }
      var degree = 0
      for (v <- comm._2) {
        val neig = neighbourhood.lookup(v)
        degree += neig.head.length
      }
      val Q = subGraph.numEdges.toDouble / numEdge - Math.pow(degree.toDouble / (2 * numEdge), 2) //(degree - subGraph.numEdges*2)/2
      if (Q > 0) {
        modularityQ += Q
      }
      print(subGraph.numEdges + " " + numEdge + " " + degree + " ")
      println(modularityQ + " (" + comm._1 + ")")
    }

    //    预测链路
    for (comm: (VertexId, ArrayBuffer[VertexId]) <- mapCommunity) {
      if (comm._2.length > graph_filter.numVertices / mapCommunity.size) { // 社区不能太小
        val subGraph = { // 创建子图
          graph_filter.subgraph(vpred = (id, communityId) => comm._2.contains(id))
        }

        //        节点度
        val subDegree = subGraph.degrees.map(a => (a._1, a._2))
        val g = subGraph.joinVertices(subDegree) {
          case (id, _, subDegree) => subDegree
        }
        g.persist()
                //        计算聚类系数
                //        找出节点的邻居节点
        val subNeigh: VertexRDD[Array[(VertexId, VertexId)]] = g.collectNeighbors(EdgeDirection.Either)
        var vertexE = new ArrayBuffer[(VertexId, Long)]()

        for (vertex <- g.vertices.collect()) {
          val array: Array[VertexId] = subNeigh.lookup(vertex._1).head.map(a => a._1)
          if (array.length > 1) {
            val neigSubGraph = {
              g.subgraph(vpred = (id, communityId) => array.contains(id))
            }

            vertexE ++= Array((vertex._1, neigSubGraph.numEdges))
          }
          else {
            vertexE ++= Array((vertex._1, 0L))
          }
        }
        val graph: Graph[(VertexId, VertexId), VertexId] = g.outerJoinVertices(sc.parallelize(vertexE))((_, old, deg) => (old, deg.getOrElse(0)))

        val pw = new java.io.PrintWriter(path + partition + "subCommVertex"  + comm._1 + ".txt")
        pw.write(ToGexf.toVertex(graph))
        pw.close
        val pw1 = new java.io.PrintWriter(path + partition +  "subCommEdge" + comm._1 + ".txt")
        pw1.write(ToGexf.toEdge(graph))
        pw1.close
      }
//        val graphSubNeigh = graph.collectNeighbors(EdgeDirection.Either)
//
//
//        var potentialEdge = new ArrayBuffer[(VertexId, VertexId, Double)]() //(srcId, dstId, Probability)
//        //        两个节点之间连接的概率
//        for (vertex1 <- graph.vertices.collect()) {
//          for (vertex2 <- graph.vertices.collect()) {
//            if (vertex1._1 < vertex2._1 && vertex1._2._1 > 8 && vertex2._2._1 > 8) {
//              //              看他们是否已经存在边
//              val vertex1Neigh = graphSubNeigh.lookup(vertex1._1).head
//              val vertex1NeighArray = vertex1Neigh.map(a => a._1)
//              var cluster = 0.0 // 两个节点连成边的概率
//              if (!vertex1NeighArray.contains(vertex2._1)) {
//                val vertex2Neigh = graphSubNeigh.lookup(vertex2._1).head
//                val coNeig: Array[(VertexId, (VertexId, VertexId))] = vertex2Neigh.union(vertex1Neigh) // 共邻节点
//                for (a <- coNeig) {
//                  //                  val neighVertex = findVertex(graph, a)
//                  if (a._2._1 > 1) {
//                    cluster += 2 * a._2._2.toDouble / (a._2._1 * a._2._1 * (a._2._1 - 1)) + 1.0 / a._2._1
//                    //                    cluster += 2 * neighVertex._2._2.toDouble / (neighVertex._2._1 * neighVertex._2._1 * (neighVertex._2._1 - 1)) + 1.0 / neighVertex._2._1
//                  }
//                }
//              }
//
//              if (cluster > 0) {
//                potentialEdge ++= Array((vertex1._1, vertex2._1, cluster))
//                println(vertex1._1 + " " + vertex2._1 + " " + cluster)
//              }
//            }
//
//          }
//        }
//        g.unpersist()
//        println("hhh")
//      }
    }

    println("模块度：" + modularityQ)
    val end_time = new Date().getTime
    println("耗时: " + (end_time - start_time))
    sc.hadoopConfiguration.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true)
    deleteOutPutPath(sc, path + "vertexComm_" + partition)
    //    deleteOutPutPath(sc, path + "neighbourhood")
    sc.parallelize(graph_filter.vertices.collect()).repartition(1).saveAsTextFile(path + "vertexComm_" + partition)
    val pw = new java.io.PrintWriter(path + "myGraph" + partition + ".gexf")
    pw.write(ToGexf.toGexf(graph_filter))
    pw.close
  }
}
