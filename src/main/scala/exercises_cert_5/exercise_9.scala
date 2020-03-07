package exercises_cert_5

/** Question 93
  * Problem Scenario 36 : You have been given a file named /home/cloudera/files/data.csv (type,name).
  * data.csv
  * 1,Lokesh
  * 2,Bhupesh
  * 2,Amit
  * 2,Ratan
  * 2,Dinesh
  * 1,Pavan
  * 1,Tejas
  * 2,Sheela
  * 1,Kumar
  * 1,Venkat
  * 1. Load this file from hdfs and save it back as (id, (all names of same type)) in results directory. However, make sure while saving it should be only one file.
  *
  * $ gedit /home/cloudera/files/data.csv
  * $ hdfs dfs -put /home/cloudera/files/data.csv /user/cloudera/files
  * $ hdfs dfs -cat /user/cloudera/files/data.csv
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

  val input = "hdfs://quickstart.cloudera/user/cloudera/files/data.csv"
  val output = "hdfs://quickstart.cloudera/user/cloudera/exercises/question_93"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val data = sc
        .textFile(input)
        .map(line => line.split(","))
        .map(arr => (arr(0), arr(1)))
        .groupByKey()
        .map({case(id, names) => (id, "(%s)".format(names.mkString(",")))})
        .repartition(1)
        .saveAsTextFile(output)

      // $ hdfs dfs -ls /user/cloudera/exercises/question_93
      // $ hdfs dfs -cat /user/cloudera/exercises/question_93/part-00000

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
