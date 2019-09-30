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
	val warehouseLocation = "hdfs://quickstart.cloudera/user/hive/warehouse"
    val spark = SparkSession.builder()
	.appName("exercise_5")
	.master("local[*]")
	.enableHiveSupport()
	.config("spark.sql.warehouse.dir", warehouseLocation)
	.getOrCreate()

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

	// 3. Calculate total revenue perday and per order
	val selectData = joined.map({case( (id,(date,subtotal))) => ((id,date),subtotal)})
	val result = selectData.reduceByKey( (v,c) => v + c)
	result.collect.foreach(println)    	

	println("**********************************")

	// 4. Calculate total and average revenue for each date. - combineByKey - aggregateByKey
	val selectData2 = joined.map({case( (id,(date, subtotal)) ) => (date, (subtotal,1))})

	// reduceByKey
	val redByKey = selectData2.reduceByKey( (v,c) => (v._1 + c._1, v._2 + c._2)).mapValues({case(s,n) => (s, s / n)}).sortByKey()
	redByKey.take(10).foreach(println)

	println("********************************")

	// combineByKey
	val combByKey = selectData2.combineByKey( ( (v: (Double, Int)) => (v._1, v._2)) , (  ( c: (Double, Int),v: (Double, Int) ) => (c._1 + v._1, c._2 + v._2) ) , ( (v: (Double, Int),c: (Double, Int)) => (v._1 + c._1, v._2 + c._2) ) )
	combByKey.mapValues({case(s, n) => (s, s / n)}).sortByKey().take(10).foreach(println)

	println("********************************")

	// aggregateByKey
	val aggByKey = selectData2.aggregateByKey((0.0,0))(((z:(Double,Int), v:(Double,Int)) => (z._1 + v._1,z._2 + v._2)),((v:(Double,Int),c:(Double,Int)) => (v._1 + c._1, v._2 + c._2))).mapValues({case(s,n) => (s, s/n)})
	aggByKey.sortByKey(false).take(10).foreach(println)

	println("********************************")

	// SPARK-SQL SOLUTION
	import spark.implicits._

	val ordersDF = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/tables/orders").map(line => line.split(",")).map(r => (r(0).toInt,r(1).substring(0,10))).toDF("id_order","date")
	val orderItemsDF = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/tables/order_items").map(line => line.split(",")).map(r => (r(1).toInt,r(4).toFloat)).toDF("item_id_order", "subtotal")

	// create temporary view
	ordersDF.createOrReplaceTempView("orders")
	orderItemsDF.createOrReplaceTempView("order_items")

	// 3. Calculate total revenue perday and per order
	val resultDF = spark.sql("""SELECT date, id_order, ROUND(SUM(subtotal),2) AS total_revenue FROM orders join order_items ON(id_order = item_id_order) GROUP BY date, id_order ORDER BY date""")
	resultDF.show()

	println("**************************************************")

	// 4. Calculate total and average revenue for each date
	val result2DF = spark.sql("""SELECT date, ROUND(SUM(subtotal),2) AS total_revenue, ROUND(AVG(subtotal),2) AS avg_revenue FROM orders JOIN order_items ON(id_order = item_id_order) GROUP BY date ORDER BY date """)
	result2DF.show()

    sc.stop()
    spark.stop()
  }
}
