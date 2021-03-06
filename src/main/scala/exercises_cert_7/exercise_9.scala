package exercises_cert_7

/**
Question 7: Correct
PreRequiste:
[PreRequiste will not be there in actual exam]
Run below sqoop command to import customer table from mysql into hdfs to the destination /user/cloudera/problem2/customer/tab

sqoop import \
--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
--password cloudera \
--username root \
--table customers \
--target-dir /user/cloudera/problem2/customer/tab \
--fields-terminated-by "\t" \
--columns "customer_id,customer_fname,customer_state" \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

Instructions:
Provided tab delimited file, get total numbers customers in each state whose first name starts with 'M' and save results in HDFS in json format.

Input folder
/user/cloudera/problem2/customer/tab

Output Requirement:
Result should be saved in /user/cloudera/problem2/customer_json_new.
Output should have state name followed by total number of customers in that state.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// spark.eventLog.enabled = true
// dependencies
// toDebugString
// getCheckpointFile
// getStorageLevel
// isCheckpointed

object exercise_9 {

  val spark = SparkSession
    .builder()
    .appName("exercise_9")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_9")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/problem2/customer/"

  def main(args: Array[String]): Unit = {
    try {

      Logger.getRootLogger.setLevel(Level.ERROR)

      val schema = StructType(List(StructField("id", IntegerType, false), StructField("fname",StringType, false), StructField("state", StringType, false)))

      val customers = sqlContext
        .read
        .schema(schema)
        .option("sep","\t")
        .csv(s"${rootPath}tab")
        .cache()

      customers.createOrReplaceTempView("customers")

      // get total numbers customers in each state whose first name starts with 'M' and save results in HDFS in json format.
      val output = sqlContext
          .sql(
            """SELECT state, COUNT(fname) AS count_names
              |FROM customers
              |WHERE fname LIKE("M%")
              |GROUP BY state
            """.stripMargin)

      output
          .toJSON
          .rdd
          .saveAsTextFile(s"${rootPath}customer_json_new")

      // TODO: check the results
      // hdfs dfs -cat /user/cloudera/problem2/customer/customer_json_new/part* | head -n 10

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
