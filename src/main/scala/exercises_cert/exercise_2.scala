package exercises_cert

/** Question 2
  * Problem Scenario 81 : You have been given MySQL DB with following details. You have
  * been given following product.csv file
  * product.csv
  * productID,productCode,name,quantity,price
  * 1001,PEN,Pen Red,5000,1.23
  * 1002,PEN,Pen Blue,8000,1.25
  * 1003,PEN,Pen Black,2000,1.25
  * 1004,PEC,Pencil 2B,10000,0.48
  * 1005,PEC,Pencil 2H,8000,0.49
  * 1006,PEC,Pencil HB,0,9999.99
  * Now accomplish following activities.
  * 1. Create a Hive ORC table using SparkSql
  * 2. Load this data in Hive table.
  * 3. Create a Hive parquet table using SparkSQL and load data in it.
  */

// Previous steps
// Move the file from local to HDFS
// $ hdfs dfs -put /home/cloudera/files/product.csv /user/cloudera/files
// $ hdfs dfs -cat /user/cloudera/files/product.csv

import exercises_cert.exercise_1.{sc, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object exercise_2 {

  val warehouseLocation = "/user/hive/warehouse"
  val path = "hdfs://quickstart.cloudera/user/cloudera/"
  val spark = SparkSession.builder()
    .appName("exercise_2")
    .master("local")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir",warehouseLocation)
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_2")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val schema = StructType(List(StructField("id",IntegerType, false), StructField("code",StringType,false),
        StructField("name",StringType,false), StructField("quantity", IntegerType), StructField("price", DoubleType)))

      val productDF = sqlContext
        .read
        .schema(schema)
        .option("header", true)
        .option("sep", ",")
        .csv(s"${path}files/product.csv")
        .cache

      productDF
        .write
        .orc(s"${path}exercise_2/orc")

      sqlContext.sql("use default")

      sqlContext.sql(
        s"""CREATE EXTERNAL TABLE IF NOT EXISTS product_orc(id int, code string, name string, quantity int, price double)
          |STORED AS ORC
          |LOCATION "${path}exercise_2/orc" """.stripMargin)

      sqlContext.sql("""SELECT * FROM product_orc""").show()

      productDF
        .write
        .parquet(s"${path}exercise_2/parquet")

      sqlContext.sql(
        s"""CREATE EXTERNAL TABLE IF NOT EXISTS product_parquet(id int, code string, name string, quantity int, price double)
          |STORED AS PARQUET
          |LOCATION "${path}exercise_2/parquet" """.stripMargin)

      sqlContext.sql("""SELECT * FROM product_parquet""").show()

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

