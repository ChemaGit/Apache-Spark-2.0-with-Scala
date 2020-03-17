package exercises_cert_7

/**
  * Question 2: Correct
  * PreRequiste:
  *[Prerequisite section will not be there in actual exam]
  *Run below sqoop command to import products table from mysql into hive table product_new:
  **
 sqoop import \
  *--connect jdbc:mysql://quickstart.cloudera/retail_db \
  *--username root \
  *--password cloudera \
  *--table products \
  *--hive-import \
  *--create-hive-table \
  *--hive-database default \
  *--hive-table product_new \
  *--outdir /home/cloudera/outdir \
  *--bindir /home/cloudera/bindir
  **
 Instructions:
  *Get products from metastore table named "product_new" whose price > 100 and save the results in HDFS in parquet format.
  *Output Requirement:
  *Result should be saved in /user/cloudera/practice1/problem8/output as parquet file
  *Files should be saved in Gzip compression.
  **
 [You will not be provided with any answer choice in actual exam.Below answers are just provided to guide you]
  *Important Information:
  **
 In case hivecontext does not get created in your environment or table not found issue occurs.
  *Just check that SPARK_HOME/conf has hive_site.xml copied from /etc/hive/conf/hive_site.xml.
  *If in case any derby lock issue occurs, delete SPARK_HOME/metastore_db/dbex.lck to release the lock.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object exercise_6 {
  val spark = SparkSession
    .builder()
    .appName("exercise_6")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_6")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/practice1/problem8/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      sqlContext.sql("""USE default""")
      sqlContext
          .sql(
            s"""CREATE EXTERNAL TABLE IF NOT EXISTS products(
              |  id INT,
              |  category_id INT,
              |  name STRING,
              |  description STRING,
              |  price DOUBLE,
              |  image STRING)
              |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
              |STORED AS TEXTFILE
              |LOCATION '${rootPath}products'
            """.stripMargin)

      sqlContext
        .sql("SHOW tables")
        .show()

      val output =sqlContext
          .sql(
            """SELECT *
              |FROM products
              |WHERE price > 100""".stripMargin)
          .cache

      sqlContext
          .setConf("spark.sql.parquet.compression.codec","gzip")

      output
          .write
          .parquet(s"${rootPath}output")

      sqlContext
          .sql("""DROP TABLE products""")

      sqlContext
          .sql("SHOW tables")
          .show()

      // Check the results
      // hdfs dfs -ls /user/cloudera/practice1/problem8/output
      // parquet-tools -meta hdfs://quickstart.cloudera/user/cloudera/practice1/problem8/output/part-00003-0dbd5a4f-2b8f-4b38-a1f4-69f4889df8fd-c000.gz.parquet

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    }finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}
