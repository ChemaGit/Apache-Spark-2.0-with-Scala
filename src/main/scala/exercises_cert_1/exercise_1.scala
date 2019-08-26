package exercises_cert_1

/** Question 15
  * Problem Scenario 80 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.products
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of products table : (product_id | product_category_id | product_name |
  * product_description | product_price | product_image )
  * Please accomplish following activities.
  * 1. Copy "retaildb.products" table to hdfs in a directory p93_products
  * 2. Now sort the products data sorted by product price per category, use productcategoryid
  * colunm to group by category
  */
// Previous steps
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

object exercise_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 1").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filt = List(""," ")
    val products = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/exercise_10/products")
      .map(line => line.split(","))
      .filter(arr => !filt.contains(arr(4)))
      .map(arr => ( (arr(1).toInt, arr(4).toFloat),arr.mkString(",")))

    val sorted = products.sortByKey()
    sorted.collect.foreach({case(k,v) => println(v)})

    sc.stop()
    spark.stop()
  }
}

