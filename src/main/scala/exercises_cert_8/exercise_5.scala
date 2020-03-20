package exercises_cert_8

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Question 4: Correct
*PreRequiste:
*Run below sqoop command to import orders table from mysql into hdfs to the destination /user/cloudera/problem4_ques6/input as parquet file.

 sqoop import \
*--connect "jdbc:mysql://localhost/retail_db" \
*--password cloudera \
*--username root \
*--table orders \
*--target-dir /user/cloudera/problem4_ques6/input \
*--delete-target-dir \
*--as-parquetfile \
*--bindir /home/cloudera/bindir \
*--outdir /home/cloudera/outdir

 Instructions:
*Save the data to hdfs using no compression as sequence file.
*Output Requirement:
*Result should be saved in at /user/cloudera/problem4_ques6/output and fields should be seperated by pipe delimiter
*/

object exercise_5 {

  val spark = SparkSession
    .builder()
    .appName("exercise_4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_4")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/problem4_ques6/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val orders = sqlContext
          .read
          .parquet(s"${path}input")
          .cache()

      orders.show(10)

      orders
          .rdd
          .map(r => (r(0).toString, r.mkString("|")))
          .saveAsSequenceFile(s"${path}output")

      // TODO: check the results
      // hdfs dfs -ls /user/cloudera/problem4_ques6/output

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

