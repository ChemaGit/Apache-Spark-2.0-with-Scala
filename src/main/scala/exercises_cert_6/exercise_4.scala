package exercises_cert_6

/** Question 97
  * Use retail_db data set
  *
  * Problem Statement
  * Get daily revenue by product considering completed and closed orders.
  * Data need to be sorted by ascending order by date and descending order
  * by revenue computed for each product for each day.
  * Data for orders and order_items is available in HDFS /public/retail_db/orders and /public/retail_db/order_items
  * Data for products is available under /public/retail_db/products
  * Final output need to be stored under
  * HDFS location-avro format /user/cloudera/question97/daily_revenue_avro
  * HDFS location-text format /user/cloudera/question97/daily_revenue_txt
  * Local location /home/cloudera/daily_revenue_scala
  * Solution need to be stored under /home/cloudera/daily_revenue_scala
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table orders \
--as-textfile \
--target-dir /public/retail_db/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table order_items \
--as-textfile \
--target-dir /public/retail_db/order_items \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table products \
--as-textfile \
--target-dir /public/retail_db/products \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir
 */
object exercise_4 {

  val spark = SparkSession
    .builder()
    .appName("exercise_4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_4")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  case class Orders(order_id: Int, order_date: String)
  case class OrderItems(order_item_order_id: Int, order_item_product_id: Int, order_item_subtotal: Double)
  case class Products(product_id: Int, product_name: String)

  val bList = sc.broadcast(List("COMPLETE", "CLOSED"))

  val input = "hdfs://quickstart.cloudera/public/retail_db/"
  val output = "hdfs://quickstart.cloudera/user/cloudera/question97/"

  def main(args: Array[String]): Unit = {
    try {

      Logger.getRootLogger.setLevel(Level.ERROR)

      import spark.implicits._

      val ordersDF = sc
        .textFile(s"${input}orders")
        .map(line => line.split(","))
        .filter(r => bList.value.contains(r(3)))
        .map(arr => Orders(arr(0).toInt, arr(1).substring(0, 10)))
        .toDF()
        .cache()

      val orderItemsDF = sc
        .textFile(s"${input}order_items")
        .map(line => line.split(","))
        .map(arr => OrderItems(arr(1).toInt, arr(2).toInt, arr(4).toDouble))
        .toDF()
        .cache()

      val productsDF = sc
        .textFile(s"${input}products")
        .map(line => line.split(","))
        .map(arr => Products(arr(0).toInt, arr(2)))
        .toDF()
        .cache()

      ordersDF.createOrReplaceTempView("orders")
      orderItemsDF.createOrReplaceTempView("order_items")
      productsDF.createOrReplaceTempView("products")

      val result = spark
        .sqlContext
        .sql(
          """SELECT product_id, product_name,order_date,ROUND(SUM(order_item_subtotal),2) AS daily_revenue
            |FROM orders JOIN order_items ON(order_id = order_item_order_id) JOIN products ON(order_item_product_id = product_id)
            | GROUP BY product_id,product_name,order_date
            | ORDER BY order_date ASC,daily_revenue DESC """.stripMargin)
        .cache()

      result.show(10)

      /*Final output need to be stored under
      HDFS location-avro format /user/cloudera/question97/daily_revenue_avro
      HDFS location-text format /user/cloudera/question97/daily_revenue_txt
     */

      import com.databricks.spark.avro._
      result
        .write
        .avro(s"${output}daily_revenue_avro")

      result
          .write
          .option("sep",",")
          .option("header", false)
          .csv(s"${output}daily_revenue_txt")

      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("Stopped SparkContext")
      spark.stop()
      println("Stopped SparkSession")
    }
  }
}
