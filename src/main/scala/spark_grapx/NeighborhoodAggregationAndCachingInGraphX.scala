package spark_grapx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import collection.mutable.HashMap

object NeighborhoodAggregationAndCachingInGraphX {
  val spark = SparkSession
    .builder()
    .appName("NeighborhoodAggregationAndCachingInGraphX")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","NeighborhoodAggregationAndCachingInGraphX") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(Array((1L, ("Billy Bill", "Person")), (2L, ("Jacob Johnson", "Person")), (3L, ("Stan Smith", "Person")), (4L, ("Homer Simpson", "Person")), (5L, ("Clark Kent", "Person")), (6L, ("James Smith", "Person"))))
      val edgeRDD: RDD[Edge[String]] = sc.parallelize(Array(Edge(2L, 1L, "Friends"), Edge(2L, 5L, "Friends"), Edge(3L, 1L, "Friends"), Edge(3L, 4L, "Friends"), Edge(3L, 5L, "Friends"), Edge(3L, 6L, "Friends"), Edge(4L, 6L, "Friends"), Edge(5L, 1L, "Friends"), Edge(5L, 1L, "Friends"), Edge(5L, 6L, "Friends"), Edge(6L, 7L, "Friends")))
      val defaultvertex = ("Self", "Missing")
      val facebook = Graph(vertexRDD, edgeRDD, defaultvertex)

      for (triplet <- facebook.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is ")
        print(triplet.attr)
        print(" with ")
        println(triplet.dstAttr._1)
      }

      val facebook_step1 = facebook.subgraph(vpred = (id, user_type) => user_type._2 != "Missing")

      for (triplet <- facebook_step1.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is ")
        print(triplet.attr)
        print(" with ")
        println(triplet.dstAttr._1)
      }

      val facebook_step2 = facebook_step1.partitionBy(PartitionStrategy.EdgePartition1D)

      val facebook_fixed = facebook_step2.groupEdges(merge = (edge1, edge2) => edge1)

      for (triplet <- facebook_fixed.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is ")
        print(triplet.attr)
        print(" with ")
        println(triplet.dstAttr._1)
      }

      val vertexRDD_old: RDD[(Long, (String, String))] = sc.parallelize(Array((1L, ("Billy Bill", "Person")), (2L, ("Jacob Johnson", "Person")), (3L, ("Andrew Smith", "Person")), (4L, ("Iron Man Fan Page", "Page")), (5L, ("Captain America Fan Page", "Page"))))
      val edgeRDD_old: RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "Friends"), Edge(1L, 3L, "Friends"), Edge(2L, 4L, "Follower"), Edge(2L, 5L, "Follower"), Edge(3L, 5L, "Follower")))
      val defaultvertex1 = ("Self", "Missing")
      val facebook_old = Graph(vertexRDD_old, edgeRDD_old, defaultvertex1)

      val facebook_compared = facebook_fixed.mask(facebook_old)

      for (triplet <- facebook_compared.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is ")
        print(triplet.attr)
        print(" with ")
        println(triplet.dstAttr._1)
      }

      val facebook_compared1 = facebook_old.mask(facebook_fixed)

      for (triplet <- facebook_compared1.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" is ")
        print(triplet.attr)
        print(" with ")
        println(triplet.dstAttr._1)
      }

      for (rankee <- facebook_fixed.pageRank(0.1).vertices.collect) {
        println(rankee)
      }

      val aggregated_vertices = facebook_fixed.aggregateMessages[Int](triplet => triplet.sendToDst(1), (a, b) => (a + b))

      for (aggregated_vertex <- aggregated_vertices.collect) {
        println(aggregated_vertex)
      }

      for (aggregated_vertex <- facebook_fixed.aggregateMessages[Int](triplet => (triplet.sendToSrc(1), triplet.sendToDst(1)), (a, b) => (a + b)).collect) {
        println(aggregated_vertex)
      }

      facebook_fixed.cache()

      /*******REAL DATA EXAMPLE******/
        // Download the file here: https://ibm.box.com/s/c805roaenqk9i220q0t48wxgw2adz6ed

      val airport_data = scala.io.Source.fromFile("/home/cloudera/files/airports.csv")
//      for (line <- airport_data.getLines()) {
//        println(line)
//      }

//      airport_data.take(10).foreach(println)

      val vertices = ArrayBuffer[(Long, (String, String, String))]()
      val vertice_map = new HashMap[String,Long]()  { override def default(key:String) = 0 }

      var counter = 1
      for (line <- airport_data.getLines()) {
        val cols = line.split(",")
        if (cols(4) != "\\N" && cols(4) != "") {
          vertice_map += (cols(4) -> counter)
          vertices += ((counter, (cols(4), cols(1), cols(3))))
          counter += 1
        }
      }
      for (line <- vertices) {
        println(line)
      }
      val plane_vertexRDD: RDD[(Long, (String, String, String))] = sc.parallelize(vertices)

      // download it here: https://ibm.box.com/s/ess4s648mwyod5f3swmcnegy5b67hith
      val route_data = scala.io.Source.fromFile("/home/cloudera/files/routes.csv")

      val edges = ArrayBuffer[Edge[String]]()

      for (line <- route_data.getLines()) {
        val cols = line.split(",")
        if (vertice_map(cols(2)) != 0 || vertice_map(cols(4)) != 0) {
          edges += (Edge(vertice_map(cols(2)), vertice_map(cols(4)), cols(0)))
        }
      }
      for (line <- edges) {
        println(line)
      }
      val plane_edgeRDD: RDD[Edge[String]] = sc.parallelize(edges)

      val default_plane_vertex = ("Location", "Currently", "Unknown")

      val plane_graph = Graph(plane_vertexRDD, plane_edgeRDD, default_plane_vertex)

      for (triplet <- plane_graph.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" to ")
        print(triplet.dstAttr._1)
        print(" with Arline ")
        println(triplet.attr)
      }

      val plane_graph_fixed = plane_graph.subgraph(vpred = (id, user_type) => user_type._1 != "Location")

      for (triplet <- plane_graph_fixed.triplets.collect) {
        print(triplet.srcAttr._1)
        print(" to ")
        print(triplet.dstAttr._1)
        print(" with Arline ")
        println(triplet.attr)
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
