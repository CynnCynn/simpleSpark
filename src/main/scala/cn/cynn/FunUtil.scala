package cn.cynn
import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object FunUtil {

    def main(args: Array[String]): Unit = {
//      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//
//      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext._
//
//    val logFile = "D:\\Scala\\doc\\LICENSE.md"  // Should be some file on your server.
// Creates a SparkSession.
      val spark = SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}").master("local[*]")
        .getOrCreate()
//          val spark = SparkSession
//        .builder
//        .master("local[*]")
//        .appName(s"${this.getClass.getSimpleName}")
//        .getOrCreate()
      val sc = spark.sparkContext
      sc.setLogLevel(Level.WARN.toString)
// --------------------------------------------------------------------
//      根据有属性的顶点和边创建图
//      顶点RDD
      val users: RDD[(VertexId, (String, String))] =
        sc.textFile("D:\\study\\课件\\毕业设计\\spark书籍\\Spark GraphX构建社交信任网络\\数据\\vertices.txt")
        .map{line=>val lines=line.split(" ");
          (lines(0).toLong, (lines(1), lines(2)))}

//      边RDD[起点ID, 目标点ID, 边的属性(边的标注, 边的权重等)], ID必须为Long
      val relationShips: RDD[Edge[String]] =
        sc.textFile("D:\\study\\课件\\毕业设计\\spark书籍\\Spark GraphX构建社交信任网络\\数据\\edges.txt")
        .map{line=>val lines = line.split(" ");
        Edge(lines(0).toLong, lines(1).toLong, lines(2))}

      //      定义一个默认缺失用户
      val defaultUser = ("John Doe", "Missing")

      //      使用RDD建立一个Graph
      val graph_urelate = Graph(users, relationShips, defaultUser)
      graph_urelate.vertices.collect.foreach(println(_))
//      case模式匹配的顶点视图
      graph_urelate.vertices.map{
        case (id, (name, prop)) =>
          (prop, name)
      }.collect.foreach(println(_))
//      增加过滤条件的顶点视图
      graph_urelate.vertices.filter{
        case (id, (name, pos)) =>
          pos == "postdoc"
      }.collect.foreach(println(_))
//      通过下标查询
      graph_urelate.vertices.map{
        v=>(v._1, v._2._1, v._2._2)
      }.collect.foreach(println(_))
//      直接查看所有边的信息
      println(graph_urelate.edges)
      graph_urelate.edges.collect.foreach(println(_))
//      通过模式匹配结构并且添加过滤条件
      graph_urelate.edges.filter{
        case Edge(srcId,dstId,prop)=>srcId>dstId
      }.collect.foreach(println(_))
//      通过下标查询
      graph_urelate.edges.map{
        e=>(e.attr, e.srcId, e.dstId)
      }.collect.foreach(println(_))

//      三元组
      println("三元组")
      println(graph_urelate.triplets)
      graph_urelate.triplets.collect.foreach(println(_))
      graph_urelate.triplets.map{
        triplet => (triplet.srcAttr._1, triplet.dstAttr._2, triplet.srcId, triplet.dstId)
      }.collect.foreach(println(_))

//      度分布
      graph_urelate.degrees.collect.foreach(println(_))
      graph_urelate.inDegrees.collect.foreach(println(_))
      graph_urelate.outDegrees.collect.foreach(println(_))

//      最大值，最小值和超级节点
      val degree2 = graph_urelate.degrees.map(a=>(a._2, a._1))
      print("max degree = " + (degree2.max()._2, degree2.max()._1))
      print("min degree = " + (degree2.min()._2, degree2.min()._1))
      degree2.sortByKey(true, 1).top(3).foreach(x => println(x._2, x._1))

//
      val newGraph = graph_urelate.mapVertices((id, attr) => attr._1)
      newGraph.vertices.collect.foreach(println(_))

      val newVertices = graph_urelate.vertices.map{
        case (id, attr) => (id, attr._1)
      }
      val newGraph1 = Graph(newVertices, graph_urelate.edges)
      newGraph1.vertices.collect.foreach(println(_))

//      修改边属性
      graph_urelate.mapEdges(e => e.srcId+e.attr+e.dstId).edges.collect.foreach(println(_))

//      将边的属性用起点的属性的第一个值(姓名)替换
      val newGraph2 = graph_urelate.mapTriplets(triplet => triplet.srcAttr._1)
      newGraph2.triplets.collect.foreach(println(_))

      val graph2 = graph_urelate.reverse
      graph2.triplets.collect.foreach(println(_))
// --------------------------------------------------------------------
//      根据边创建图
      val records1: RDD[String] = sc.textFile("D:\\study\\课件\\毕业设计\\spark书籍\\Spark GraphX构建社交信任网络\\数据\\edges.txt")
      val followers1=records1.map {case x => val fields=x.split(" ")
        Edge(fields(0).toLong, fields(1).toLong,fields(2))
      }
      val graph_fromEdges = Graph.fromEdges(followers1, 1L)
      graph_fromEdges.vertices.collect.foreach(println(_))
      graph_fromEdges.edges.collect.foreach(println(_))
// -------------------------------------------------------------------------

      //根据边的两个顶点的二元组创建图
      val file = sc.textFile("D:\\study\\课件\\毕业设计\\spark书籍\\Spark GraphX构建社交信任网络\\数据\\edges.txt")
      val edgeRDD: RDD[(VertexId, VertexId)] = file.map(line => line.split(" "))
        .map(line=>(line(0).toLong, line(1).toLong))
//      创建图
      val graph_fromEdgeTuples = Graph.fromEdgeTuples(edgeRDD, 1)
      println(graph_fromEdges.numEdges)
      graph_fromEdgeTuples.vertices.collect.foreach(println(_))
      graph_fromEdgeTuples.edges.collect.foreach(println(_))
//      -----------------------------------------------------------------------

//      val sparkConf = new SparkConf().setAppName("SparkSessionZipsExample").setMaster("local")
//          // your handle to SparkContext to access other context like SQLContext
//          val sc = new SparkContext(sparkConf)
//    val logData = sc.textFile(logFile, 2).cache()
//    val numAs = logData.filter(line => line.contains("h")).count()
//    val numBs = logData.filter(line => line.contains("j")).count()
//    println("Lines with h: %s, Lines with j: %s".format(numAs, numBs))
//    val rdd = sc.makeRDD (1 to 10 , 3)
//    rdd.preferredLocations(rdd.partitions(0)
      val path : String = "D:\\study\\课件\\毕业设计\\spark书籍\\Spark GraphX构建社交信任网络\\数据\\soc-Epinions1.txt\\"
      val records: RDD[String] = sc.textFile(path + "soc-Epinions1.txt")
      val followers=records.map {case x =>val fields=x.split("\t")
          Edge(fields(0).toLong, fields(1).toLong,1L )
      }
      val graph=Graph.fromEdges(followers, 1L)

      // 求需要支付稿酬的用户
      val indegrees_graph = graph.inDegrees.map(a=>(a._2,a._1)).sortByKey(false,1)
      val indegrees_graph_top = indegrees_graph.top(50).map(a=>(a._2,a._1))
      sc.hadoopConfiguration.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true)
      deleteOutPutPath(sc, path + "need_pay")
      sc.parallelize(indegrees_graph_top).repartition(1).saveAsTextFile(path + "need_pay")

//      // 找出信任度在前 3%的用户顶点
//      // 找出信任用户
      val numUsers = graph.numVertices
      val numXinren =math.round(numUsers*0.03).toInt
      val Xinren_User: RDD[(VertexId, PartitionID)] = sc.parallelize(indegrees_graph.top(numXinren).map(a=>(a._2,a._1)))


          //
      // 标记信任度前 3%的用户
      // 更改图中的信任用户顶点属性
      val graph_join =graph.outerJoinVertices(Xinren_User){
          case(id,prop,Some(energy))=>("trust")
          case(id,prop,None)=>(1)
      }


      //找出上榜用户
      //找出活跃用户
      val olderFollowers=graph_join.aggregateMessages[Int](
          triplet => {
              if(triplet.srcAttr.equals("trust")){
                  triplet.sendToSrc(1)}},
          (a,b)=>(a+b),
      TripletFields.All)
      val huoyue = olderFollowers.map(a=>(a._2,a._1)).sortByKey(false)
      val num = math.round(huoyue.count*0.05)
      val shangbang_user: Array[VertexId] = huoyue.top(num.toInt).map(a=>(a._2))
      deleteOutPutPath(sc, path + "shangbang_user")
//      存储结果
      sc.parallelize(shangbang_user).repartition(1).saveAsTextFile(path + "shangbang_user")
  }

//  删除文件操作
  def deleteOutPutPath(sc: SparkContext, outputPath: String):Unit={
    val path = new Path(outputPath)
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(path)){
      hdfs.delete(path,true)
    }
  }

}
