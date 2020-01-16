package exercises_cert_5

import org.apache.spark.sql.SparkSession

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

object exercise_10 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 10")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    // USING RDD
    val ordersRdd = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/tables/orders/")
        .map(line => line.split(","))
        .map(arr => (arr(0).toInt, (arr(1), arr(2).toInt)))

    val orderItemsRdd = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/tables/order_items/")
        .map(line => line.split(","))
        .map(arr => (arr(1).toInt, arr(4).toDouble))

    val joinedRdd = ordersRdd
        .join(orderItemsRdd)
        .persist()

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
    import spark.implicits._
    val ordersDF = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/tables/orders/")
        .map(line => line.split(","))
        .map(arr => (arr(0).toInt, arr(1), arr(2).toInt))
        .toDF("idOrder", "date", "custId")

    val itemOrdersDF = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/tables/order_items/")
        .map(line => line.split(","))
        .map(arr => (arr(1).toInt, arr(4).toDouble))
        .toDF("idOrderIt", "revenue")

    ordersDF.createOrReplaceTempView("orders")
    itemOrdersDF.createOrReplaceTempView("item_orders")

    // 3. Calculate total revenue perday and per customer
    spark
        .sqlContext
        .sql("""SELECT date, custId, ROUND(SUM(revenue),2) AS Total_Revenue FROM orders JOIN item_orders ON(idOrder = idOrderIt) GROUP BY date, custId ORDER BY Total_Revenue DESC""")
        .show()

    // 4. Calculate maximum revenue per customer
    spark
        .sqlContext
        .sql("""SELECT custId, ROUND(SUM(revenue), 2) AS Max_Revenue_Customer FROM orders JOIN item_orders ON(idOrder = idOrderIt) GROUP BY custId ORDER BY Max_Revenue_Customer DESC LIMIT 1""")
        .show()

    sc.stop()
    spark.stop()
  }

}
