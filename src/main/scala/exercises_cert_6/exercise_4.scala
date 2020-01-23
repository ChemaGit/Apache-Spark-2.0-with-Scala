package exercises_cert_6

import org.apache.spark.sql.SparkSession

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

  lazy val spark = SparkSession
    .builder()
    .appName("exercise_4")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  case class Orders(order_id: Int, order_date: String)
  case class OrderItems(order_item_order_id: Int, order_item_product_id: Int, order_item_subtotal: Double)
  case class Products(product_id: Int, product_name: String)

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val bList = sc.broadcast(List("COMPLETE", "CLOSED"))

    val ordersDF = sc
      .textFile("hdfs://quickstart.cloudera/public/retail_db/orders")
      .map(line => line.split(","))
      .filter(r => bList.value.contains(r(3)))
      .map(arr => Orders(arr(0).toInt,arr(1).substring(0,10)))
      .toDF()
      .persist()

    val orderItemsDF = sc
      .textFile("hdfs://quickstart.cloudera/public/retail_db/order_items")
      .map(line => line.split(","))
      .map(arr => OrderItems(arr(1).toInt, arr(2).toInt, arr(4).toDouble))
      .toDF()
      .persist()

    val productsDF = sc
      .textFile("hdfs://quickstart.cloudera/public/retail_db/products")
      .map(line => line.split(","))
      .map(arr => Products(arr(0).toInt,arr(2)))
      .toDF()
      .persist()

    ordersDF.createOrReplaceTempView("orders")
    orderItemsDF.createOrReplaceTempView("order_items")
    productsDF.createOrReplaceTempView("products")

    val result = spark
        .sqlContext
        .sql(
          """SELECT product_id, product_name,order_date,ROUND(SUM(order_item_subtotal),2) AS daily_revenue FROM orders JOIN order_items ON(order_id = order_item_order_id)
            | JOIN products ON(order_item_product_id = product_id) GROUP BY product_id,product_name,order_date ORDER BY order_date ASC,daily_revenue DESC """.stripMargin)

    // result.show(10)

    /*Final output need to be stored under
      HDFS location-avro format /user/cloudera/question97/daily_revenue_avro
      HDFS location-text format /user/cloudera/question97/daily_revenue_txt
     */

    import com.databricks.spark.avro._
    result
        .write
        .avro("hdfs://quickstart.cloudera/user/cloudera/question97/daily_revenue_avro")

    result
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/question97/daily_revenue_txt")

    sc.stop()
    spark.stop()
  }
}
