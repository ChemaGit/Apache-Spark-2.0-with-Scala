package spark_grapx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object ExploringGrapX {
  val spark = SparkSession
    .builder()
    .appName("ExploringGrapX")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","ExploringGrapX") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val facebook_vertices = Array((1L, ("Billy Bill", "Person")), (2L, ("Jacob Johnson", "Person")),
        (3L, ("Andrew Smith", "Person")), (4L, ("Iron Man Fan Page", "Page")),
        (5L, ("Captain America Fan Page", "Page")))

      val relationships = Array(Edge(1L, 2L, "Friends"), Edge(1L, 3L, "Friends"), Edge(2L, 4L, "Follower"), Edge(2L, 5L, "Follower"), Edge(3L, 5L, "Follower"))

      val vertexRDD: RDD[(Long, (String, String))] = sc.parallelize(facebook_vertices)
      val edgeRDD: RDD[Edge[String]] = sc.parallelize(relationships)

      val defaultvertex = ("Self", "Missing")

      val facebook = Graph(vertexRDD, edgeRDD, defaultvertex)

      print(facebook.vertices)

      print(facebook.edges)

      println(facebook.vertices.filter { case (id, (name, user_type)) => user_type == "Person" }.count)

      println(facebook.edges.filter { case (relation) => relation.dstId == 5L && relation.attr == "Follower"}.count)

      val selected = facebook.triplets.filter { case (triplet) => triplet.srcAttr._1 == "Billy Bill"}.collect

      for (person <- selected) {
        print(person.srcAttr._1)
        print(" is ")
        print(person.attr)
        print(" with ")
        println(person.dstAttr._1)
      }

      val characters: RDD[(VertexId, (String, String))] = sc.parallelize(Array((1L, ("Homer Simpson", "Person")), (2L, ("Bart Simpson", "Person")),
        (3L, ("Marge Simpson", "Person")), (4L, ("Milhouse Houten", "Page"))))

      val simpson_relationships : RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "Father"), Edge(3L, 1L, "Wife"), Edge(2L, 4L, "Friends")))

      val the_simpsons = Graph(characters, simpson_relationships, defaultvertex)

      println(the_simpsons.vertices)

      println(the_simpsons.edges)

      val selected1 = facebook.triplets.filter { case (triplet) => triplet.srcAttr._1 == "Homer Simpson"}.collect

      for (person <- selected1) {
        print(person.srcAttr._1)
        print(" is ")
        print(person.attr)
        print(" with ")
        println(person.dstAttr._1)
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
