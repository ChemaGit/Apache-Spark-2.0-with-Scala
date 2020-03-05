package exercises_cert

/** Question 14
  * Problem Scenario 79 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.products
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of products table : (product_id | product categoryid | product_name | product_description | product_prtce | product_image )
  * Please accomplish following activities.
  * 1. Copy "retaildb.products" table to hdfs in a directory p93_products
  * 2. Filter out all the empty prices
  * 3. Sort all the products based on price in both ascending as well as descending order.
  * 4. Sort all the products based on price as well as product_id in descending order.
  * 5. Use the below functions to do data ordering or ranking and fetch top 10 elements top() takeOrdered() sortByKey()
  */

// previous steps
/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table products \
--as-textfile \
--delete-target-dir \
--target-dir /public/retail_db/products/ \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \

hdfs dfs -ls /public/retail_db/products/
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object exercise_10 {

  val spark = SparkSession
    .builder()
    .appName("exercise 10")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_10")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "hdfs://quickstart.cloudera/public/retail_db/products/"

  def main(args: Array[String]): Unit = {

    try {
      Logger.getRootLogger.setLevel(Level.ERROR)

      val filt = List("", " ")
      val bcv = sc.broadcast(filt)

      val products = sc
        .textFile(input)
        .map(line => line.split(","))
        .filter(arr => bcv.value.contains(arr(4)) == false)
        .cache()


      val productsPrice = products
        .map(arr => (arr(4).toFloat, arr))
        .cache()

      productsPrice
        .sortByKey()
        .collect
        .foreach(t => println(t._1, t._2.mkString("[",",","]")))

      productsPrice
        .sortByKey(false)
        .collect
        .foreach(t => println(t._1, t._2.mkString("[",",","]")))

      products
        .map(arr => ( (arr(4).toFloat, arr(0).toInt), arr.mkString("[",",","]")))
        .sortByKey(false)
        .collect.foreach(x => println("%s => %s".format(x._1,x._2.mkString(""))))
      println()

      products
        .top(10)(Ordering[Float].reverse.on(arr => arr(4).toFloat))
        .foreach(x => println(x.mkString(",")))
      println()

      products
        .top(10)(Ordering[Float].on(arr => arr(4).toFloat))
        .foreach(x => println(x.mkString(",")))
      println()

      products
        .top(10)(Ordering[Float].on(arr => -arr(4).toFloat))
        .foreach(x => println(x.mkString(",")))
      println()

      products
        .takeOrdered(10)(Ordering[Float].reverse.on(arr => arr(4).toFloat))
        .foreach(x => println(x.mkString(",")))
      println()

      products
        .takeOrdered(10)(Ordering[Float].on(arr => arr(4).toFloat))
        .foreach(x => println(x.mkString(",")))
      println()

      products
        .takeOrdered(10)(Ordering[Float].on(arr => -arr(4).toFloat))
        .foreach(x => println(x.mkString(",")))
      println()

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