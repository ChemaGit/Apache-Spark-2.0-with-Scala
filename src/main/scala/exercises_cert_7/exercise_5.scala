package exercises_cert_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
Question 1: Correct
PreRequiste:[Prerequisite section will not be there in actual exam. Your exam environment will already be setup with required data]
Run below sqoop command

sqoop import \
--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
--password cloudera \
--username root \
--table customers \
--fields-terminated-by '\t' \
--columns "customer_id,customer_fname,customer_city" \
--target-dir /user/cloudera/problem9/customer_text \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

Instructions:
Create a metastore table named customer_parquet_compressed from tab delimited files provided at below location.
Input folder is /user/cloudera/problem9/customer-text
Schema for input file
customer_id customer_fname customer_city

Output Requirement:
Use this location to store data for hive table: /user/cloudera/problem9/customer-hive
Output file should be saved in parquet format using Snappy compression.

Important Information
I have provided the solution using Hive. You can also solve it using Spark+Hive.
For compression, below Hive property should be set to true
SET hive.exec.compress.output=true;
*/

object exercise_5 {

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

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/problem9/customer_text"

  def main(args: Array[String]): Unit = {

    try {
      Logger.getRootLogger.setLevel(Level.ERROR)

      val schema = StructType(List(StructField("customer_id",IntegerType, false), StructField("customer_fname",StringType,false),StructField("customer_city",StringType,false)))

      val customers = sqlContext
          .read
          .option("sep", "\t")
          .schema(schema)
          .csv(rootPath)
          .cache

      customers.createOrReplaceTempView("customers")

      sqlContext.sql("""USE default""")

      sqlContext
          .sql(
            """CREATE TABLE IF NOT EXISTS customer_parquet_compressed
              |STORED AS PARQUET
              |LOCATION "hdfs://quickstart.cloudera/user/cloudera/problem9/customer-hive"
              | TBLPROPERTIES("parquet.compression"="snappy")
              | AS SELECT * FROM customers""".stripMargin)

      sqlContext
          .sql("""SELECT * FROM customer_parquet_compressed LIMIT 10""")
          .show(10)


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
