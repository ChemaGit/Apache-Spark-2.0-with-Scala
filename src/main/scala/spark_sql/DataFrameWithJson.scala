package spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object DataFrameWithJson {

  lazy val spark = SparkSession
    .builder()
    .appName("Data Frame With Json")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","DataFrameWithJson") // To silence Metrics warning
    .getOrCreate()
  lazy val sc = spark.sparkContext
  lazy val sqlContext = spark.sqlContext

  var out = true

  def main(args: Array[String]): Unit = {
    try {
      sc.setLogLevel("ERROR")

      val json: DataFrame = sqlContext.read.json("hdfs://quickstart.cloudera/user/cloudera/files/employee.json")
      // spark infers the schema as it reads the json document. Since there is a invalid
      // json record the schema will have an additional column called _corrupt_record
      // for invalid json record.
      // It doesn't stop the processing when it finds an invalid records which is great for
      // large jobs as you don't want to stop for each invalid data record

      json.printSchema()
      println("Loaded carrier information")
      json.show(10)

      // Printing out the records that failed to parse
      Try(json.where(json("_corrupt_record").isNotNull).collect()) match {
        case Success(value) => value.foreach(println)
        case Failure(exception) => println("There is no corrupts records")
      }

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