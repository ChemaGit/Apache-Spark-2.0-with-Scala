package exercises_cert_2

/** Question 46
  * Problem Scenario 74 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of order table : (orderid , order_date , ordercustomerid, order_status}
  * Columns of order_items table : (order_item_td , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Please accomplish following activities.
  * 1. Copy "retaildb.orders" and "retaildb.order_items" table to hdfs in respective directory question46/orders and question46/order_items .
  * 2. Join these data using orderid in Spark and Scala
  * 3. Now fetch selected columns from joined data Orderid, Order_date and amount collected on this order.
  * 4. Calculate total order placed for each date, and produced the output sorted by date.
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
--num-mappers 8

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
--num-mappers 8
*/

import org.apache.spark.sql._

object exercise_7 {

  def main(args: Array[String]): Unit = {
	val warehouseLocation = "hdfs://quickstart.cloudera/user/hive/warehouse"
	val spark = SparkSession
			.builder()
			.appName("Exercise 7")
			.master("local[*]")
			.enableHiveSupport()
			.config("spark.sql.warehouse.dir", warehouseLocation)
			.getOrCreate()
	val sc = spark.sparkContext
	sc.setLogLevel("ERROR")

	// SPARK-RDD SOLUTION
	val orders = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/tables/orders").map(line => line.split(",")).map(r => (r(0).toInt, r(1)))
	val orderItems = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/tables/order_items").map(line => line.split(",")).map(r => (r(1).toInt, r(4).toFloat))

	val joined = orders.join(orderItems).map({case((id,(date,subtotal))) => ((id,date.substring(0,10)),subtotal)})

	val ordersPerDate = joined.groupByKey().map({case(((id, date),iter)) => (date,1)}).reduceByKey((v,c) => v + c).sortByKey()
	ordersPerDate.take(10).foreach(println)

	println()
		println("***************")
		println()

	// SPARK-SQL SOLUTION
	import spark.implicits._

	val ordersDF = orders.toDF("id","date")
	val orderItemsDF = orderItems.toDF("id","subtotal")

	ordersDF.createOrReplaceTempView("o")
	orderItemsDF.createOrReplaceTempView("oi")
	val joinedDF = spark.sql("""SELECT o.id, date, subtotal FROM o JOIN oi ON(o.id = oi.id) """)
	joinedDF.createOrReplaceTempView("j")
	val distinctIdDate = spark.sql("""SELECT date, id FROM j  GROUP BY date, id """)
	distinctIdDate.createOrReplaceTempView("dd")
	val ordersPerDateDF = spark.sql("""SELECT substr(date, 0, 10) AS date, COUNT(id) AS total_orders FROM dd GROUP BY date ORDER BY date""")
	ordersPerDateDF.show(10)

	sc.stop()
	spark.stop()
  }

}
