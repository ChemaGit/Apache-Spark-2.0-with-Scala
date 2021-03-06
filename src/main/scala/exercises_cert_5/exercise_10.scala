package exercises_cert_5

/** Question 94
  * Problem Scenario 78 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of order table : (orderid , order_date , order_customer_id, order_status)
  * Columns of order_items table : (order_item_td , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Please accomplish following activities.
  * 1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory question94/orders and question94/order_items .
  * 2. Join these data using order_id in Spark and Scala
  * 3. Calculate total revenue perday and per customer
  * 4. Calculate maximum revenue customer
  *
  * $ sqoop import \
  * --connect jdbc:mysql://quickstart.cloudera/retail_db \
  * --username retail_dba \
  * --password cloudera \
  * --table orders \
  * --as-textfile \
  * --delete-target-dir \
  * --target-dir /user/cloudera/tables/orders \
  * --outdir /home/cloudera/outdir \
  * --bindir /home/cloudera/bindir
  *
  * sqoop import \
  * --connect jdbc:mysql://quickstart.cloudera/retail_db \
  * --username retail_dba \
  * --password cloudera \
  * --table order_items \
  * --as-textfile \
  * --delete-target-dir \
  * --target-dir /user/cloudera/tables/order_items \
  * --outdir /home/cloudera/outdir \
  * --bindir /home/cloudera/bindir
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object exercise_10 {

  val spark = SparkSession
    .builder()
    .appName("exercise_10")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_10")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/tables/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // USING RDD
      val ordersRdd = sc
        .textFile(s"${path}orders/")
        .map(line => line.split(","))
        .map(arr => (arr(0).toInt, (arr(1), arr(2).toInt)))

      val orderItemsRdd = sc
        .textFile(s"${path}order_items/")
        .map(line => line.split(","))
        .map(arr => (arr(1).toInt, arr(4).toDouble))

      val joinedRdd = ordersRdd
        .join(orderItemsRdd)
        .cache()

      // 3. Calculate total revenue perday and per customer
      // (Int, ((String, Int), Double))

      val revenuePerdayCustomer = joinedRdd
        .map({case( (id,((date,cust), revenue)) ) => ( (date, cust), revenue)})
        .reduceByKey( (v, c) => v + c)
        .sortBy(t => t._2, false)

      revenuePerdayCustomer
        .take(25)
        .foreach(println)

      // 4. Calculate maximum revenue per customer
      val maxRevenueCustomer = joinedRdd
        .map({case( (id,((date,cust), revenue)) ) => (cust, revenue)})
        .reduceByKey( (v, c) => v + c)
        .sortBy(t => t._2, false)
        .first()

      println(s"Maximum revenue per customer: $maxRevenueCustomer")

      // USING DATAFRAMES

      val ordersSchema = StructType(List(StructField("order_id", IntegerType, false), StructField("order_date",StringType, false),
        StructField("order_customer_id", IntegerType, false), StructField("order_status",StringType, false)))
      //(order_item_td , order_item_order_id , order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
      val orderItemsSchema = StructType(List(StructField("item_id", IntegerType, false), StructField("item_order_id",IntegerType, false),
        StructField("item_product_id", IntegerType, false), StructField("item_quantity",IntegerType, false),
        StructField("item_subtotal",DoubleType, false), StructField("item_price",DoubleType, false)))

      val ordersDF = sqlContext
        .read
        .schema(ordersSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}orders/")
        .cache()

      val itemOrdersDF = sqlContext
        .read
        .schema(orderItemsSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}order_items/")
        .cache()

      ordersDF.createOrReplaceTempView("orders")
      itemOrdersDF.createOrReplaceTempView("item_orders")

      // 3. Calculate total revenue perday and per customer
      sqlContext
        .sql(
          """SELECT order_date, order_customer_id, ROUND(SUM(item_subtotal),2) AS Total_Revenue
            |FROM orders JOIN item_orders ON(order_id = item_order_id)
            |GROUP BY order_date, order_customer_id
            |ORDER BY Total_Revenue DESC""".stripMargin)
        .show()

      // 4. Calculate maximum revenue per customer
      sqlContext
        .sql(
          """SELECT order_customer_id, ROUND(SUM(item_subtotal), 2) AS Max_Revenue_Customer
            |FROM orders JOIN item_orders ON(order_id = item_order_id)
            |GROUP BY order_customer_id
            |ORDER BY Max_Revenue_Customer DESC LIMIT 1""".stripMargin)
        .show()

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}
