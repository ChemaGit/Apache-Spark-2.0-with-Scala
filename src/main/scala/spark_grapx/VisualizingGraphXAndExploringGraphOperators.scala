package spark_grapx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object VisualizingGraphXAndExploringGraphOperators {

  val spark = SparkSession
    .builder()
    .appName("VisualizingGraphXAndExploringGraphOperators")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","VisualizingGraphXAndExploringGraphOperators") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(Array((1L, ("Billy Bill", "Person")),
        (2L, ("Jacob Johnson", "Person")), (3L, ("Andrew Smith", "Person")),
        (4L, ("Iron Man Fan Page", "Page")), (5L, ("Captain America Fan Page", "Page"))))

      val edgeRDD: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "Friends"), Edge(1L, 3L, "Friends"), Edge(2L, 4L, "Follower"), Edge(2L, 5L, "Follower"), Edge(3L, 5L, "Follower")))

      val defaultvertex = ("Self", "Missing")

      val facebook = Graph(vertexRDD, edgeRDD, defaultvertex)

      println(facebook.numVertices)

      println(facebook.numEdges)

      val Billy_outDegree = facebook.outDegrees.filter{ case(id, outdegree) => id == 1}.collect

      print(Billy_outDegree(0))

      val Billy_inDegree = facebook.inDegrees.filter{ case(id, outdegree) => id == 1}.collect
      // print(Billy_inDegree(0)) IndexOutBoundException

      for (degree <- facebook.degrees.collect)
      {
        println(degree)
      }

      for (vertex <- facebook.vertices.collect) {
        println(vertex)
      }

      for (edge <- facebook.edges.collect) {
        println(edge)
      }

      for (triplet <- facebook.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is a ")
        print(triplet.attr)
        print(" of ")
        println(triplet.dstAttr._1)
      }

      val rank = facebook.pageRank(0.1).vertices.collect

      for (rankee <- rank) {
        println(rankee)
      }

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped")
      spark.stop()
      println("SparkSession stopped")
    }
  }
}
