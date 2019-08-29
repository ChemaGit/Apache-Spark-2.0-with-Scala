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
--target-dir /user/cloudera/exercise_10/products \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

hdfs dfs -ls /user/cloudera/exercise_10/products
*/

import org.apache.spark.sql._

object exercise_10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 10").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filt = List("", " ")
    val products = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/exercise_10/products/part-m-00000").map(line => line.split(",")).filter(arr => !filt.contains(arr(4)))
    val productsPrice = products.map(arr => (arr(4).toFloat, arr))

    val productsAsc = productsPrice.sortByKey()
    productsAsc.collect.foreach(t => println(t._1, t._2.mkString("[",",","]")))

    val productsDesc = productsPrice.sortByKey(false)
    productsDesc.collect.foreach(t => println(t._1, t._2.mkString("[",",","]")))

    val priceAndId = products.map(arr => ( (arr(4).toFloat, arr(0).toInt), arr.mkString("[",",","]")))
    val priceAndIdDesc = priceAndId.sortByKey(false)
    priceAndIdDesc.collect.foreach(x => println("%s => %s".format(x._1,x._2.mkString(""))))
    println()

    val topPrice = products.top(10)(Ordering[Float].reverse.on(arr => arr(4).toFloat))
    topPrice.foreach(x => println(x.mkString(",")))
    println()

    val topPrice1 = products.top(10)(Ordering[Float].on(arr => arr(4).toFloat))
    topPrice1.foreach(x => println(x.mkString(",")))
    println()

    val topPrice2 = products.top(10)(Ordering[Float].on(arr => -arr(4).toFloat))
    topPrice2.foreach(x => println(x.mkString(",")))
    println()

    val takeOrdered = products.takeOrdered(10)(Ordering[Float].reverse.on(arr => arr(4).toFloat))
    takeOrdered.foreach(x => println(x.mkString(",")))
    println()

    val takeOrdered1 = products.takeOrdered(10)(Ordering[Float].on(arr => arr(4).toFloat))
    takeOrdered1.foreach(x => println(x.mkString(",")))
    println()

    val takeOrdered2 = products.takeOrdered(10)(Ordering[Float].on(arr => -arr(4).toFloat))
    takeOrdered2.foreach(x => println(x.mkString(",")))
    println()

    sc.stop()
    spark.stop()
  }
}