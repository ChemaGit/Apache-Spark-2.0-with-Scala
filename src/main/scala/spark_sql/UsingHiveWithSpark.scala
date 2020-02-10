package spark_sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, round}

/**
  * # HIVECONTEXT
  * 	- SparkSQL supports two SQL dialects:
  * 		- It's own, a subset of HiveQL
  * 		- HiveQL - integrated with Hive metastores
  * 	- The HiveQL SQL is richer and is preferred when doing more extensive data processing with SQL from Spark
  *
  * 	- To use Hive, instantiate a HiveContext instead of a SQLContext --> Deprecated (Since version 2.0.0) Use SparkSession.builder.enableHiveSupport instead)
  */
object UsingHiveWithSpark {

  val spark = SparkSession
    .builder()
    .appName("DataFrameJoins")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","DataFrameJoins") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    // Hive wants the full path
    val path = "hdfs://quickstart.cloudera/public/retail_db/products/"

    try {
      // External means Hive doesn't own the data. It's in the location we specify.
      sqlContext
        .sql("""SHOW databases""")
        .show()
      sqlContext
        .sql("""SHOW tables""")
        .show()

      sqlContext
        .sql(s"""
              CREATE EXTERNAL TABLE IF NOT EXISTS products (
              product_id int,
              product_category_id int,
              product_name string,
              product_description string,
              product_price double,
              product_image string)
              ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
              LOCATION '$path'
            """).show

      sqlContext
        .sql("""SHOW tables""")
        .show()
      sqlContext
        .sql("""DESCRIBE products""")
        .show()
      sqlContext
        .sql("""DESCRIBE EXTENDED products""")
        .show(100)
      sqlContext
        .sql("""DESCRIBE FORMATTED products""")
        .show(100)
      sqlContext
        .sql("""DESCRIBE FORMATTED products""")
        .show(100, truncate = false)
      sqlContext
        .sql("""DESCRIBE FORMATTED products""")
        .foreach(x => println(x.toString()))
      sqlContext
        .sql("""SHOW CREATE TABLE products""")
        .show(100, truncate = false)

      sqlContext
        .sql("""SELECT COUNT(*) FROM products""")
        .show()

      sqlContext
        .sql(
          """SELECT product_category_id, SUM(product_price) AS sum_products
            |FROM products
            |GROUP BY product_category_id
            |ORDER BY sum_products DESC""".stripMargin)
        .show()

      sqlContext
        .sql(
          """SELECT product_name, product_description, product_price
            |FROM products
            |WHERE product_price > 99
            |ORDER BY product_price DESC""".stripMargin)
        .show()
      // Results are DataFrames, so we can use that API
      val catPrice = sqlContext
        .sql("""SELECT product_category_id, product_price FROM products""")

      catPrice
        .cube(col("product_category_id"))
        .agg(round(avg(col("product_price")),2).as("avg_price"))
        .orderBy(col("product_category_id"))
        .show()

      sqlContext
        .sql("""DROP TABLE products""")

      sqlContext
        .sql("""SHOW TABLES""")
        .show()


      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped")
      spark.stop()
      println("SparkSession stopped")
    }
  }

}