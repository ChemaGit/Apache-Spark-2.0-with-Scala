package exercises_cert_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Question 5: Correct
  * PreRequiste:
  * [PreRequiste will not be there in actual exam]
*sqoop import \
*--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
*--password cloudera \
*--username root \
*--table orders \
*--fields-terminated-by "\t" \
*--target-dir /user/cloudera/practice2/problem3/orders \
*--outdir /home/cloudera/outdir \
*--bindir /home/cloudera/bindir
 **
 sqoop import \
*--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
*--password cloudera \
*--username root \
*--table order_items \
*--fields-terminated-by "\t" \
*--target-dir /user/cloudera/practice2/problem3/order_items \
*--outdir /home/cloudera/outdir \
*--bindir /home/cloudera/bindir
 **
 sqoop import \
*--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
*--password cloudera \
*--username root \
*--table customers \
*--fields-terminated-by "\t" \
*--target-dir /user/cloudera/practice2/problem3/customers \
*--outdir /home/cloudera/outdir \
*--bindir /home/cloudera/bindir
 **
Instructions
Get all customers who have placed order of amount more than 200.

Input files are tab delimeted files placed at below HDFS location:
/user/cloudera/practice2/problem3/customers
/user/cloudera/practice2/problem3/orders
/user/cloudera/practice2/problem3/order_items

Schema for customers File
Customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode

Schema for Orders File
Order_id,order_date,order_customer_id,order_status

Schema for Order_Items File
Order_item_id,Order_item_order_id,order_item_product_id,Order_item_quantity,Order_item_subtotal,Order_item_product_price

Output Requirements:
>> Output should be placed in below HDFS Location
/user/cloudera/practice2/problem3/joinResults
>> Output file should be comma seperated file with customer_fname,customer_lname,customer_city,order_amount
  */

object exercise_7 {

  val spark = SparkSession
    .builder()
    .appName("exercise 4")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_5")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/practice2/problem3/"

  def main(args: Array[String]): Unit = {

    try {
      Logger.getRootLogger.setLevel(Level.ERROR)

      val schema = StructType(List(StructField("customer_id",IntegerType, false), StructField("customer_fname",StringType,false),StructField("customer_city",StringType,false)))
      val schemaOrders = ???
      val schemaOrderItems = ???
      val schemaCustomers = ???

      val orders = sqlContext
          .read
          .schema(schemaOrders)
          .option("sep","\t")
          .csv(s"${rootPath}orders")

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
