package exercises_cert_2

/** Question 43
  * Problem Scenario 77 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of order table : (orderid , order_date , order_customer_id, order_status)
  * Columns of ordeMtems table : (order_item_id , order_item_order_ld ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Please accomplish following activities.
  * 1. Copy "retail_db.orders" and "retail_db.order_items" table to hdfs in respective directory p92_orders and p92_order_items .
  * 2. Join these data using orderid in Spark and Scala
  * 3. Calculate total revenue perday and per order
  * 4. Calculate total and average revenue for each date. - combineByKey -aggregateByKey
  */

/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--as-textfile \
--delete-target-dir \
--target-dir /user/cloudera/tables/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table order_items \
--as-textfile \
--delete-target-dir \
--target-dir /user/cloudera/tables/order_items \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1


 */

import org.apache.spark.sql._

object exercise_5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise_5").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val orders = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/tables/orders")
        .map(line => line.split(","))
        .map(arr => (arr(0).toInt, arr(1).substring(0,10)))

    val orderItems = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/tables/order_items")
        .map(line => line.split(","))
        .map(arr => (arr(1).toInt, arr(4).toDouble))

    val joined = orders.join(orderItems)

    joined.take(20).foreach(println)

    sc.stop()
    spark.stop()
  }
}
