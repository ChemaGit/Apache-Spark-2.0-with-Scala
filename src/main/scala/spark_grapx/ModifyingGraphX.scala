package spark_grapx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object ModifyingGraphX {

  val spark = SparkSession
    .builder()
    .appName("ModifyingGraphX")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","ModifyingGraphX") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    sc.setLogLevel("ERROR")

    try {

      val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(Array((1L, ("Billy Bill", "Person")), (2L, ("Jacob Johnson", "Person")),
        (3L, ("Andrew Smith", "Person")), (4L, ("Iron Man Fan Page", "Page")), (5L, ("Captain America Fan Page", "Page"))))

      val edgeRDD: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "Friends"), Edge(1L, 3L, "Friends"), Edge(2L, 4L, "Follower"), Edge(2L, 5L, "Follower"), Edge(3L, 5L, "Follower")))

      val defaultvertex = ("Self", "Missing")

      val facebook = Graph(vertexRDD, edgeRDD, defaultvertex)

      val facebook_temp = facebook.mapVertices((id, user_type) => if (id == 1) ("Billy D. Bill", "Person") else user_type)

      for (vertex <- facebook_temp.vertices.collect) {
        println(vertex)
      }

      val facebook_temp2 = facebook_temp.mapVertices((id, user_type) => if (id == 5) ("Captain America Fan Page is the Best!", "Page") else user_type)

      for (vertex <- facebook_temp2.vertices.collect) {
        println(vertex)
      }

      val facebook_temp3 = facebook_temp2.mapEdges((edge) => if (edge.srcId == 3) "Supreme Follower" else edge.attr)

      for (edges <- facebook_temp3.edges.collect) {
        println(edges)
      }

      val facebook_temp4 = facebook_temp3.mapEdges((edge) => if (edge.attr == "Friends") "Friend" else edge.attr)
      for (edges <- facebook_temp4.edges.collect) {
        println(edges)
      }

      val facebook_temp5 = facebook_temp4.mapTriplets((triplet) => if (triplet.srcAttr._1 == "Billy Bill" && triplet.dstAttr._1 == "Jacob Johnson") "Best-Friend" else triplet.attr)

      for (triplet <- facebook_temp5.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is a ")
        print(triplet.attr)
        print(" of ")
        println(triplet.dstAttr._1)
      }

      val facebook_temp6 = facebook_temp5.reverse

      for (triplet <- facebook_temp6.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is a ")
        print(triplet.attr)
        print(" of ")
        println(triplet.dstAttr._1)
      }
      println("Page Rank values --------------")
      for (rankee <- facebook_temp6.pageRank(0.1).vertices.collect) {
        println(rankee)
      }

      val facebook_subgraph = facebook_temp6.subgraph(vpred = (id, user_type) => user_type._2 == "Person")

      for (triplet <- facebook_subgraph.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is a ")
        print(triplet.attr)
        print(" of ")
        println(triplet.dstAttr._1)
      }

      for (triplet <- facebook.subgraph(epred = (edgetriplet) => edgetriplet.attr == "Follower").triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is a ")
        print(triplet.attr)
        print(" of ")
        println(triplet.dstAttr._1)
      }

      for (triplet <- facebook.mask(facebook_subgraph).triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is a ")
        print(triplet.attr)
        print(" of ")
        println(triplet.dstAttr._1)
      }

      val vertexRDD2: RDD[(Long, (String, String))] = sc.parallelize(Array((1L, ("Billy Bill", "Person")), (2L, ("Jacob Johnson", "Person")), (3L, ("Andrew Smith", "Person"))))
      val edgeRDD2: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "Friends"), Edge(1L, 3L, "Friends"), Edge(1L, 3L, "Friends")))
      val simple_facebook = Graph(vertexRDD2, edgeRDD2, defaultvertex)

      for (triplet <- simple_facebook.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is ")
        print(triplet.attr)
        print(" with ")
        println(triplet.dstAttr._1)
      }

      val simple_facebook_partitioned = simple_facebook.partitionBy(PartitionStrategy.EdgePartition1D)

      val new_simple_facebook = simple_facebook_partitioned.groupEdges(merge = (edge1, edge2) => edge1)

      for (triplet <- new_simple_facebook.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is ")
        print(triplet.attr)
        print(" with ")
        println(triplet.dstAttr._1)
      }

      val vertexRDD5: RDD[(Long, (String, String))] = sc.parallelize(Array((1L, ("Billy Bill", "Person")), (2L, ("Jacob Johnson", "Person")), (3L, ("Stan Smith", "Person")), (4L, ("Homer Simpson", "Person")), (5L, ("Clark Kent", "Person")), (6L, ("James Smith", "Person"))))
      val edgeRDD5: RDD[Edge[String]] = sc.parallelize(Array(Edge(2L, 1L, "Friends"), Edge(2L, 5L, "Friends"), Edge(3L, 1L, "Friends"), Edge(3L, 4L, "Friends"), Edge(3L, 5L, "Friends"), Edge(3L, 6L, "Friends"), Edge(4L, 6L, "Friends"), Edge(5L, 1L, "Friends"), Edge(5L, 1L, "Friends"), Edge(5L, 6L, "Friends"), Edge(6L, 7L, "Friends")))
      val defaultvertex2 = ("Self", "Missing")
      val facebook2 = Graph(vertexRDD5, edgeRDD5, defaultvertex2)

      for (triplet <- facebook2.triplets.collect) {
        print(triplet.srcAttr)
        print(" is ")
        print(triplet.attr)
        print(" with ")
        println(triplet.dstAttr)
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
