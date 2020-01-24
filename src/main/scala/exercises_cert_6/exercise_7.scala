package exercises_cert_6

import org.apache.spark.sql.SparkSession

/**
  * Problem 2:
  *1. Using sqoop copy data available in mysql products table to folder /user/cloudera/products on hdfs as text file. columns should be delimited by pipe '|'
*2. move all the files from /user/cloudera/products folder to /user/cloudera/problem2/products folder
*3. Change permissions of all the files under /user/cloudera/problem2/products such that owner has read,write and execute permissions,
  *group has read and write permissions whereas others have just read and execute permissions
*4. read data in /user/cloudera/problem2/products and do the following operations using a) dataframes api b) spark sql c) RDDs aggregateByKey method.
   *Your solution should have three sets of steps.
   *Sort the resultant dataset by category id
	*- filter such that your RDD\DF has products whose price is lesser than 100 USD
	*- on the filtered data set find out the higest value in the product_price column under each category
	*- on the filtered data set also find out total products under each category
	*- on the filtered data set also find out the average price of the product under each category
	*- on the filtered data set also find out the minimum price of the product under each category
*5. store the result in avro file using snappy compression under these folders respectively
	*- /user/cloudera/problem2/products/result-df
	*- /user/cloudera/problem2/products/result-sql
	*- /user/cloudera/problem2/products/result-rdd
  */

object exercise_7 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 7")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  val inputpath = "hdfs://quickstart.cloudera/user/cloudera/problem2/products"

  def main(args: Array[String]): Unit = {
    try {
      sc.setLogLevel("ERROR")


      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("Stopped SparkContext")
      spark.stop()
      println("Stopped SparkSession")
    }
  }

}
