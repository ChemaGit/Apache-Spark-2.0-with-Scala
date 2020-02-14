package exercises_cert_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Question 4: Correct
  * Prerequiste:
  * [Prerequisite section will not be there in actual exam]
  *
  * Import orders table from mysql into hdfs location /user/cloudera/practice4/orders/.Run below sqoop statement
  * Import customers from mysql into hdfs location /user/cloudera/practice4/customers/.Run below sqoop statement
  *
  * Instructions:
  *
  * Join the data at hdfs location /user/cloudera/practice4/orders/ & /user/cloudera/practice4/customers/ to find out customers whose orders status is like "pending"
  * Schema for customer File
  * Customer_id,customer_fname,customer_lname
  * Schema for Order File
  * Order_id,order_date,order_customer_id,order_status
  *
  * Output Requirement:
  * Output should have customer_id,customer_fname,order_id and order_status.Result should be saved in /user/cloudera/practice4/output
  *
  *
  */
/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--as-textfile \
--target-dir /user/cloudera/practice4/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table customers \
--as-textfile \
--target-dir /user/cloudera/practice4/customers \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

 */

object exercise_1 {

  val spark = SparkSession
    .builder()
    .appName("exercise 1")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_10")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  case class Orders(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)
  case class Customers(customer_id: Int, customer_fname: String, customer_lname: String, customer_city: String, customer_state: String)

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/practice4/"

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      import spark.implicits._

      val orders = sc
          .textFile(s"${rootPath}orders")
          .map(line => line.split(","))
          .map(r => Orders(r(0).toInt,r(1),r(2).toInt,r(3)))
          .toDF
          .cache
      orders.show(10)

      val customers = sc
          .textFile(s"${rootPath}customers")
          .map(line => line.split(","))
          .map(r => Customers(r(0).toInt,r(1),r(2),r(6),r(7)))
          .toDF
          .cache
      customers.show(10)

      orders.createOrReplaceTempView("orders")
      customers.createOrReplaceTempView("customers")

      val output = sqlContext
          .sql(
            """SELECT c.customer_id,c.customer_fname,o.order_id,o.order_status
              |FROM orders o
              |JOIN customers c ON(o.order_customer_id = c.customer_id)
              |WHERE o.order_status LIKE("%PENDING%")
              |ORDER BY c.customer_id
            """.stripMargin)
          .cache

      output.show(10)

      output
          .write
          .option("header",true)
          .csv(s"${rootPath}output")

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
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
