package exercises_cert_8

/**
Question 7:
PreRequiste:
Run below sqoop command to import orders table from mysql into hdfs to the destination /user/cloudera/problem4_ques7/input as text file.

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table orders \
--as-textfile \
--target-dir /user/cloudera/problem4_ques7/input \
--delete-target-dir \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

Instructions:
Save the data to hdfs using no compression as orc file
Output Requirement:
Result should be saved in HDFS at /user/cloudera/problem4_ques7/output.
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

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

  val path = "hdfs://quickstart.cloudera/user/cloudera/problem4_ques7/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val orders = sqlContext
          .read
          .csv(s"${path}input")
          .cache()

      orders.show(10)

      orders
          .write
          .option("compression","none")
          .mode("overwrite")
          .orc(s"${path}output")

      // TODO: check the oputput
      // hdfs dfs -ls /user/cloudera/problem4_ques7/
      // hdfs dfs -ls /user/cloudera/problem4_ques7/output
      // hdfs dfs -text /user/cloudera/problem4_ques7/output/part-00000-d4a5a571-513c-4e6d-88a3-ea7a4526cb38-c000.orc | head -n 10

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
