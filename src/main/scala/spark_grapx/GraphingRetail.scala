package spark_grapx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, PartitionID, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}

import scala.util.hashing.MurmurHash3

object GraphingRetail {
  var quiet = true

  val spark = SparkSession
    .builder()
    .appName("GraphX")
    .master("local[*]")
    .config("spark.app.id", "GraphX") // To silence Metrics Warning
    .getOrCreate()

  val sc = spark.sparkContext

  case class Orders(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)
  case class OrderItems(item_id: Int, item_order_id: Int, item_product_id: Int, item_quantity: Int, item_subtotal: Double)
  case class Customers(customer_id: Int, customer_fname: String, customer_lname: String, customer_city: String, customer_state: String)
  case class Joined(customer_id: Int,customer_fname: String, customer_lname:String,
                    customer_city: String,customer_state: String,order_id: Int,order_date: String,
                    order_status: String,item_product_id: Int,item_quantity: Int, item_subtotal: Double)

  val inputRoot = "hdfs://quickstart.cloudera/public/retail_db/"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      import spark.implicits._

      val orders = sc
        .textFile(s"${inputRoot}orders/")
        .map(line => line.split(","))
        .map(r => Orders(r(0).toInt,r(1),r(2).toInt,r(3)))
        .toDF
        .cache

      val orderItems = sc
        .textFile(s"${inputRoot}order_items/")
        .map(line => line.split(","))
        .map(r => OrderItems(r(0).toInt, r(1).toInt, r(2).toInt, r(3).toInt, r(4).toDouble))
        .toDF
        .cache

      val customers = sc
        .textFile(s"${inputRoot}customers/")
        .map(line => line.split(","))
        .map(r => Customers(r(0).toInt,r(1),r(2),r(6),r(7)))
        .toDF
        .cache

      orders.createOrReplaceTempView("o")
      orderItems.createOrReplaceTempView("oi")
      customers.createOrReplaceTempView("c")

      val joined = spark
        .sqlContext
        .sql(
          """SELECT customer_id,customer_fname,customer_lname,customer_city,customer_state,
            |  order_id,SUBSTR(order_date,0,10) AS order_date, order_status,
            |  item_product_id,item_quantity,item_subtotal
            |FROM c JOIN o ON(c.customer_id = o.order_customer_id) JOIN oi ON(o.order_id = oi.item_order_id)
          """.stripMargin)
        .rdd
        .map(r => Joined(r(0).toString.toInt,r(1).toString,r(2).toString,r(3).toString,
          r(4).toString,r(5).toString.toInt,r(6).toString,r(7).toString,r(8).toString.toInt,r(9).toString.toInt,r(10).toString.toDouble))
        .cache

      // joined.show(10)
      // Create vertices out  for both order_id and customer_id
      val codes = joined.flatMap{f => Seq(f.order_id.toString, f.customer_id.toString)}
      val vertices: RDD[(VertexId, String)] = codes.distinct().map(x => (MurmurHash3.stringHash(x).toLong, x))

      // Create edges between order_id -> customer_id pair and the set the edge attribute
      // to count of number of orders between given a pair of order_id and customer_id
      val retailEdges = joined.map(f => ((MurmurHash3.stringHash(f.order_id.toString), MurmurHash3.stringHash(f.customer_id.toString)), 1))
        .reduceByKey(_+_)
        .map{case((src,dest),attr) => Edge(src,dest,attr)}

      val graph = Graph(vertices, retailEdges)
      if(!quiet) {
        println("\nNumber of orders and customers in the graph: ")
        println(graph.numVertices)
        println("\nNumber of cust -> orders in the graph: ")
        println(graph.numEdges)
      }

      // Show the top 10 flights between two airports
      println("\nFinding the most frequent relation between orders and customers: ")
      val triplets: RDD[EdgeTriplet[String, PartitionID]] = graph.triplets

      // Graph.triplets returns RDD of EdgeTriplet that has
      // src order_id, desc customer_id and attribute
      triplets.sortBy(_.attr, ascending=false)
        .map(triplet => s"${triplet.srcAttr} -> ${triplet.dstAttr}: ${triplet.attr}")
        .take(10).foreach(println)

      println("\nFrequent order: ")
      val by = triplets.map{triplet => (triplet.srcAttr, triplet.attr)}
        .reduceByKey(_ + _)
      by.sortBy(-_._2).take(1).foreach(println)

      // Which orders has the most unique relation to orders -> customers
      // vertices with no "in-degree" are ignored here
      val incoming: RDD[(VertexId, (PartitionID,String))] = graph.inDegrees.join(vertices)

      println("\n orders with least number of distinct customers: ")
      incoming.map{case(_,(count, ord)) => (count, ord)}
        .sortByKey().take(10).foreach(println)

      println("\nOrders with most number of distinct outgoing customers: ")
      val outgoing: RDD[(VertexId,(PartitionID, String))] = graph.outDegrees.join(vertices)

      outgoing.map{case(_,(count, ord)) => (count, ord)}
        .sortByKey(ascending = false).take(10).foreach(println)

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop
      spark.stop
    }
  }

}
