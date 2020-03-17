package exercises_cert_7

/**
Question 3: Correct
PreRequiste:
Run below sqoop command to import customer table from mysql into hdfs to the destination /user/cloudera/problem3/all/customer/input as text file and fields seperated by tab character

sqoop import \
--connect "jdbc:mysql://localhost/retail_db" \
--password cloudera \
--username root \
--table customers \
--fields-terminated-by '\t' \
--target-dir /user/cloudera/problem3/customer/input \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

Instructions:
Get input from hdfs dir /user/cloudera/problem3/customer/input and save only first 4 field in the result as pipe delimited file in HDFS

Output Requirement:
Result should be saved in /user/cloudera/problem3/customer/output Output file should be saved in text format.
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object exercise_10 {
  val spark = SparkSession
    .builder()
    .appName("exercise_10")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_10")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/problem3/customer/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val customers = sc
        .textFile(s"${rootPath}input")
        .map(line => line.split('\t'))
        .map(r => "%s|%s|%s|%s".format(r(0),r(1),r(2),r(3)))
        .saveAsTextFile(s"${rootPath}output")

      // TODO: check the results
      // hdfs dfs -cat /user/cloudera/problem3/customer/output/part* | head -n 10

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
