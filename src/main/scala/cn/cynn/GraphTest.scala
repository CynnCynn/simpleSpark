package cn.cynn

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object GraphTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}").master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel(Level.WARN.toString)
    val path = "D:\\study\\课件\\毕业设计\\spark书籍\\《Spark大数据技术与应用》源数据和代码\\Spark大数据技术与应用\\第7章\\01-任务程序\\data\\"
    val users = sc.textFile(path + "user.txt").map {
      line =>
        val lines = line.split(",")
        (lines(0).toLong, (lines(1), lines(2).toInt))
    }
    val relationships = sc.textFile(path + "relate.txt").map {
      line =>
        val lines = line.split(",")
        Edge(lines(0).toLong, lines(1).toLong, lines(2).toInt)
    }
    val graph = Graph(users, relationships)
    graph.triplets.collect.foreach(println(_))

    //    反转
    val graph2 = graph.reverse
    println("reverse")
    graph2.triplets.collect.foreach(println(_))

    //    子图
    val subGraph = graph.subgraph(
      vpred = (id, attr) => attr._2 > 30)
    println("subGraph")
    subGraph.vertices.collect.foreach(println(_))
    subGraph.edges.collect.foreach(println(_))

    val vertexs = List(1,2,3,4)
    val subGraph1 = {
      graph.subgraph(vpred = (id, attr) =>
        vertexs.contains(id))
    }
    subGraph1.vertices.collect.foreach(println(_))

    //    mask
    println("mask")
    val mask_graph = graph.mask(subGraph)
    mask_graph.vertices.collect.foreach(println(_))

    //    groupEdges
    println("groupEdges")
    val graph3 = graph.partitionBy(PartitionStrategy.RandomVertexCut)
    graph3.groupEdges((a, b) => a + b).edges.collect.foreach(println(_))

    //    collectNeighbors
    println("Out")
    val neighboors: VertexRDD[Array[(VertexId, (String, Int))]] = graph.collectNeighbors(EdgeDirection.Either)
    for (a <- neighboors) {
      print(a._1 + "[")
      for (b <- a._2) {
        print(b._1 + ",(" + b._2._1 + "," + b._2._2 + ") ")
      }
      println("]")
    }
    for(b<-neighboors.lookup(2L)){
      b.foreach(println(_))
    }
    println("In")
    graph.collectNeighbors(EdgeDirection.In).collect.foreach(println(_))
    println("Either")
    graph.collectNeighbors(EdgeDirection.Either).collect.foreach(println(_))
    val olderFollowers = graph.aggregateMessages[(Int, Int)](
      triplet => {
        triplet.sendToDst((1, triplet.srcAttr._2))
      },
      (a, b) => (a._1 + b._1, a._2 + b._2),
      TripletFields.All
    )
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues((id, value) => value match {
        case (count, totalAge)
        => totalAge / count.toDouble
      })
    avgAgeOfOlderFollowers.collect.foreach(println(_))

    val energys: VertexRDD[PartitionID] = graph.aggregateMessages[Int](
      triple => triple.sendToDst(1), (a, b) => a + b
    )
    val energys_name = graph.joinVertices(energys) {
      case (id, (name, age), energy) => (name, age + energy)
    }
    energys_name.vertices.collect.foreach(println(_))

    val graph_avgAge = graph.outerJoinVertices(avgAgeOfOlderFollowers){
      case (id, (name, age), Some(avgAge)) => (name, age, avgAge)
        case(id, (name, age), None) => (name, age, 0)
    }
    graph_avgAge.vertices.collect.foreach(println(_))
    spark.stop()
  }
}
