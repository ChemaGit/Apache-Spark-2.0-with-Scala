package spark_sql

import org.apache.spark.sql.{SparkSession}

object DataFrameWithCsv {

  lazy val spark = SparkSession
    .builder()
    .appName("Data Frame With CSV")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","DataFrameWithCsv") // To silence Metrics warning
    .getOrCreate()
  lazy val sc = spark.sparkContext
  lazy val sqlContext = spark.sqlContext

  var out = true

  def main(args: Array[String]): Unit = {
    try {
      sc.setLogLevel("ERROR")



      val df = sqlContext.read
        // Specifies the input data source format. The readers and writers
        // of this format is provided by the databricks-csv library. This also shows
        // how to add support for custom data sources.
        .format("com.databricks.spark.csv")
        .option("header","true")  // Use first line of all files as header
        .option("inferSchema", "true") // Automatically infer data types
        .load("hdfs://quickstart.cloudera/user/cloudera/files/product.csv")

      df.printSchema()
      df.show()

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      if(out) println("SparkContext stopped")
      spark.stop()
      if(out) println("SparkSession stopped")
    }
  }
}
