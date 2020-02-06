package spark_sql

import org.apache.spark.sql.SparkSession

object DataFrameJoins {

  val spark = SparkSession
    .builder()
    .appName("DataFrameJoins")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","DataFrameJoins") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  case class Orders(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)
  case class OrderItems(item_id: Int, item_order_id: Int, item_product_id: Int, item_quantity: Int, item_subtotal: Double, item_product_price: Double)

  def main(args: Array[String]): Unit = {
    val or = "hdfs://quickstart.cloudera/public/retail_db/orders/"
    val it = "hdfs://quickstart.cloudera/public/retail_db/order_items/"

    try {
      sc.setLogLevel("ERROR")

      import spark.implicits._
      import org.apache.spark.sql.functions._

      // Load the data, parse it, and construct the RDDs
      // Wrap in DataFrames and cache them.
      val orders = sc
        .textFile(or)
        .map(line => line.split(","))
        .map(arr => Orders(arr(0).toInt,arr(1),arr(2).toInt,arr(3)))
        .toDF

      orders.cache()

      println("Orders schema: ")
      orders.printSchema()

      println("Fights, first 10 records: ")
      orders.show(10) // Omit the argument(default is 20)

      val orderItems = sc
        .textFile(it)
        .map(line => line.split(","))
        .map(arr => OrderItems(arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toInt, arr(4).toDouble, arr(5).toDouble))
        .toDF

      orderItems.cache()

      println("ItemOrders schema: ")
      orders.printSchema()

      println("OrderItems, first 10 records: ")
      orderItems.show(10)

      val revenueByOrder = orderItems
        .groupBy($"item_order_id")
        .agg(count("item_id").as("num_items"),
          round(sum("item_subtotal"),2).as("total_revenue"))
        .orderBy($"total_revenue".desc,$"item_order_id")

      revenueByOrder.show(10)

      // SQL version
      orderItems.createOrReplaceTempView("order_items")
      orders.createOrReplaceTempView("orders")
      val revenueByOrderSql = sqlContext
        .sql(
          """SELECT item_order_id, COUNT(item_id) AS num_items, ROUND(SUM(item_subtotal),2) AS total_revenue
            |FROM order_items
            |GROUP BY item_order_id
            |ORDER BY total_revenue DESC, item_order_id""".stripMargin)
      revenueByOrderSql.show(10)

      revenueByOrderSql.cache()

      revenueByOrderSql.createOrReplaceTempView("revenue_by_order")

      val joined = orders
        .join(revenueByOrderSql, orders("order_id") === revenueByOrderSql("item_order_id"))
        .select("order_id", "order_date", "order_status","num_items" ,"total_revenue")
        .orderBy($"order_id")

      joined.show(10)

      // SQL version
      val joinedSql = sqlContext
        .sql(
          """SELECT order_id, order_date, order_status, num_items, total_revenue
            |FROM orders JOIN revenue_by_order ON(order_id = item_order_id)
            |ORDER BY order_id
          """.stripMargin)
      joinedSql.show(10)

      // Left Outer Join to return all order_id even if they don't have item_order_id
      val leftJoined = orders
        .join(revenueByOrderSql, orders("order_id") === revenueByOrderSql("item_order_id"),"left_outer")
        .select("order_id", "order_date", "order_status","num_items" ,"total_revenue")
        .orderBy($"order_id")

      leftJoined.show(10)

      // SQL version
      val leftJoinedSql = sqlContext
        .sql(
          """SELECT order_id, order_date, order_status, num_items, total_revenue
            |FROM orders LEFT OUTER JOIN revenue_by_order ON(order_id = item_order_id)
            |ORDER BY order_id
          """.stripMargin)

      leftJoinedSql.show(10)

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
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