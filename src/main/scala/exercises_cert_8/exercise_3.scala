package exercises_cert_8

/**
Question 7: Correct
PreRequiste:[Prerequisite section will not be there in actual exam]
Run below sqoop command

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--columns "customer_id,customer_fname,customer_city" \
--target-dir /user/cloudera/problem8/customer-avro \
--as-avrodatafile \
--bindir /home/cloudera/bindir \
--outdir /home/cloudera/outdir

Instructions:
Create a metastore table from avro files provided at below location.
Input folder is /user/cloudera/problem8/customer-avro

Output Requirement:
Table name should be customer_parquet_avro
output location for hive data /user/cloudera/problem8/customer-parquet-hive
Output file should be saved in parquet format using GZIP compression.

[You will not be provided with any answer choice in actual exam.Below answers are just provided to guide you]
To check compression of generated parquet files, use parquet tools
parquet-tools meta hdfs://quickstart.cloudera:8020/user/cloudera/problem8/customer-parquet-hive/000000_0
avro-tools getschema hdfs://cloudera@quickstart:8020/user/cloudera/problem8/customer-avro/part-m-00000.avro > cust-avro.avsc
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._

object exercise_3 {

  val spark = SparkSession
    .builder()
    .appName("exercise_3")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_3")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  val input = "hdfs://quickstart.cloudera/user/cloudera/problem8/customer-avro"
  val output = "hdfs://quickstart.cloudera/user/cloudera/problem8/customer-parquet-hive"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val customersAvro = sqlContext
          .read
          .avro(input)
          .cache()

      customersAvro.show(10)

      customersAvro
          .write
          .option("compression","gzip")
          .parquet(output)

      sqlContext.sql("""USE hadoopexam""")

      sqlContext
          .sql(
            s"""CREATE EXTERNAL TABLE IF NOT EXISTS customer_parquet_avro(
              |customer_id INT,
              |customer_fname STRING,
              |customer_city STRING
              |)
              |STORED AS PARQUET
              |LOCATION '$output'
              |TBLPROPERTIES ("parquet.compression"="gzip")
            """.stripMargin)

      sqlContext.sql("SHOW tables").show()

      sqlContext.sql("""SELECT * FROM customer_parquet_avro LIMIT 10""").show()

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
