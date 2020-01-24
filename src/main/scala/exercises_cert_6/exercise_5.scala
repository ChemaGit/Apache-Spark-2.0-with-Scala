package exercises_cert_6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

/** Question 98
  *   	- Task 1: Get revenue for given order_item_order_id
  *       	- Define function getOrderRevenue with 2 arguments order_items and order_id
  *       	- Use map reduce APIs to filter order items for given order id, to extract order_item_subtotal and add it to get revenue
  *       	- Return order revenue
  */

/*
+--------------------------+------------+------+-----+---------+----------------+
| Field                    | Type       | Null | Key | Default | Extra          |
+--------------------------+------------+------+-----+---------+----------------+
| order_item_id            | int(11)    | NO   | PRI | NULL    | auto_increment |
| order_item_order_id      | int(11)    | NO   |     | NULL    |                |
| order_item_product_id    | int(11)    | NO   |     | NULL    |                |
| order_item_quantity      | tinyint(4) | NO   |     | NULL    |                |
| order_item_subtotal      | float      | NO   |     | NULL    |                |
| order_item_product_price | float      | NO   |     | NULL    |                |
+--------------------------+------------+------+-----+---------+----------------+

sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username root \
--password cloudera \
--table order_items \
--as-textfile \
--target-dir /public/retail_db/order_items \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir
 */

object exercise_5 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 5")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  val inputpath = "hdfs://quickstart.cloudera/public/retail_db/order_items"

  def getOrderRevenue(orderId: Int, orderItems: RDD[(Int, Double)]): Double = {
    orderItems
      .filter({case(id, subtotal) => id == orderId})
      .map({case(id, subtotal) => subtotal})
      .reduce((v1, v2) => v1 + v2)
  }

  def checkOrderId(s: String): Int = Try(s.toInt) match {
    case Success(n) => {
      if(n > 0) n else 0
    }
    case Failure(_) => 0
  }

  def main(args: Array[String]): Unit = {
    try {
      println("Introduce an order_id: ")
      val orderId = checkOrderId(scala.io.StdIn.readLine())
      if (orderId > 0) {
        println(s"order_id: $orderId")

        sc.setLogLevel("ERROR")

        val order_items = sc
          .textFile(inputpath)
          .map(line => line.split(""","""))
          .map(r => (r(1).toInt,r(4).toDouble))

        val orderRevenue = getOrderRevenue(orderId,order_items)

        println(s"The total revenue for order_id $orderId is: $orderRevenue")

        println("Type whatever to the console to exit......")
        scala.io.StdIn.readLine()
      } else println(s"Invalid order_id $orderId, it must be a number greater than 0")
    } finally {
      sc.stop()
      println("Stopped SparkContext")
      spark.stop()
      println("Stopped SparkSession")
    }
  }

}
