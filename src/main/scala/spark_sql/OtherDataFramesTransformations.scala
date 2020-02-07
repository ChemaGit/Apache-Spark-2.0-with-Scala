package spark_sql

import org.apache.spark.sql.SparkSession


object OtherDataFramesTransformations {

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
    sc.setLogLevel("ERROR")

    val or = "hdfs://quickstart.cloudera/public/retail_db/orders/"
    val it = "hdfs://quickstart.cloudera/public/retail_db/order_items/"

    try {
      import spark.implicits._
      import org.apache.spark.sql.functions._

      // Load the data, parse it, and construct the RDDs
      // Wrap in DataFrames and cache them.
      val orders = sc
        .textFile(or)
        .map(line => line.split(","))
        .map(arr => Orders(arr(0).toInt,arr(1).substring(0,10),arr(2).toInt,arr(3)))
        .toDF
        .cache

      val orderItems = sc
        .textFile(it)
        .map(line => line.split(","))
        .map(arr => OrderItems(arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toInt, arr(4).toDouble, arr(5).toDouble))
        .toDF
        .cache

      val joined = orders.join(orderItems, col("order_id") === col("item_order_id"))
        .select(col("order_id"), col("order_date"),col("order_customer_id"), col("order_status"), col("item_quantity"), col("item_subtotal"))
        .cache

      joined.createOrReplaceTempView("joined")

      joined
        .groupBy(col("order_id"), col("order_date"))
        .agg(
          min(col("item_subtotal")).as("min_revenue"),
          max(col("item_subtotal")).as("max_revenue"),
          round(sum(col("item_subtotal")),2).as("total_revenue"),
          round(avg("item_subtotal"),2).as("avg_revenue"))
        .orderBy(col("total_revenue").desc, col("order_date").asc)
        .show()

      // Equivalent SQL query
      sqlContext
        .sql(
          """SELECT order_id, order_date,
            | MIN(item_subtotal) AS min_revenue,
            | MAX(item_subtotal) AS max_revenue,
            | ROUND(SUM(item_subtotal),2) AS total_revenue,
            | ROUND(AVG(item_subtotal),2) AS avg_revenue
            | FROM joined GROUP BY order_id, order_date
            | ORDER BY total_revenue DESC, order_date ASC""".stripMargin)
        .show()

      joined.agg(count("*")).show()
      joined.agg(count(col("order_id"))).show()
      joined.agg(countDistinct(col("order_id"))).show()
      joined.agg(approx_count_distinct("order_id")).show()

      // Equivalent SQL query
      sqlContext.sql("""SELECT COUNT(*) AS count FROM joined""").show()
      sqlContext.sql("""SELECT COUNT(order_id) AS count FROM joined""").show()
      sqlContext.sql("""SELECT COUNT(DISTINCT order_id) AS cd FROM joined""").show()

      // # CUBES AND ROLLUPS
      // - In data warehouses, it's common to construct multidimensional cubes and rollups for analytics,
      // suc as aggregations, in addition to using groupBy

      // Compute averages for all numeric fields, for each order_id, order_date pair
      joined
        .cube(col("order_id"), col("order_date"))
        .avg()
        .show()
      // SQL version
      sqlContext
        .sql(
          """SELECT order_id, order_date,
            |AVG(order_id) AS avg_id,
            |AVG(order_customer_id) AS avg_customer_id,
            |AVG(item_quantity) AS avg_quantity,
            |AVG(item_subtotal) AS avg_subtotal
            |FROM joined GROUP BY order_id, order_date
          """.stripMargin)
        .show()

      // Average revenue over all order_id where order_status = COMPLETE
      joined
        .cube("order_id", "order_status")
        .avg()
        .select("order_id", "order_status", "avg(item_subtotal)")
        .filter(r => r(1).toString == "COMPLETE")
        .show()
      // SQL version
      sqlContext
        .sql(
          """SELECT order_id, order_status, AVG(item_subtotal) AS avg_revenue
            |FROM joined
            |WHERE order_status = "COMPLETE"
            |GROUP BY order_id, order_status """.stripMargin)
        .show()

      // Use agg(Map()) to specify columns and functions to call. Convenient, but not flexible.

      // flights.cube($"origin", $"dest").agg(Map("*" -> "count", "times.actualElapsedTime" -> "avg", "distance" -> "avg")).orderBy($"avg(distance)".desc).show()
      joined
        .cube("order_id", "order_date")
        .agg(Map("*" -> "count", "item_subtotal" -> "avg"))
        .select("order_id", "order_date", "count(1)","avg(item_subtotal)")
        .orderBy(col("order_id"))
        .show()
      // SQL version
      sqlContext
        .sql(
          """SELECT order_id, order_date, COUNT(1) AS total_orders, AVG(item_subtotal) AS avg_revenue
            |FROM joined
            |GROUP BY order_id, order_date
            |ORDER BY order_id""".stripMargin)
        .show()

      // More flexible format
      joined.cube($"order_id", $"order_status").agg(
        avg("item_quantity").as("avg_quantity"),
        min("item_quantity").as("min_quantity"),
        max("item_quantity").as("max_quantity"),
        avg("item_subtotal").as("avg_subtotal")
      ).orderBy($"order_id".asc)
        .show()
      // Corresponding SQL version
      sqlContext
        .sql(
          """SELECT order_id, order_status,AVG(item_quantity) AS avg_quantity,
            | MIN(item_quantity) AS min_quantity,MAX(item_quantity) AS max_quantity,
            | AVG(item_subtotal) AS avg_subtotal
            | FROM joined
            | GROUP BY order_id, order_status
            | ORDER BY order_id
          """.stripMargin)
        .show()


      joined.cube($"order_customer_id", $"order_status")
        .agg(countDistinct("order_customer_id").as("count"))
        .orderBy($"count".desc)
        .show

      sqlContext
        .sql(
          """SELECT order_customer_id, order_status, COUNT(*) AS count
            | FROM joined
            | GROUP BY order_customer_id, order_status
            | ORDER BY count DESC
          """.stripMargin)
        .show()

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