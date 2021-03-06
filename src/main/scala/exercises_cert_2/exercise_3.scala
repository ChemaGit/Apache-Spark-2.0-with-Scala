package exercises_cert_2

/** Question 41
  * Problem Scenario 70 : Write down a Spark Application using Scala, In which it read a
  * file "content.txt" (On hdfs) with following content. Do the word count and save the
  * results in a directory called "problem85" (On hdfs)
  * content.txt
  * Hello this is ABCTECH.com
  * This is XYZTECH.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object exercise_3 {

  val spark = SparkSession
    .builder()
    .appName("exercise_3")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_3")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "hdfs://quickstart.cloudera/user/cloudera/files/Content.txt"
  val output = "hdfs://quickstart.cloudera/user/cloudera/question_41"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val file = sc.textFile(input)

      val wordCount = file
        .flatMap(line => line.split("\\W"))
        .map(w => (w, 1))
        .reduceByKey( (v, v1) => v + v1)
        .sortBy(t => t._2, false)

      wordCount
        .foreach(println)

      wordCount
        .saveAsTextFile(output)

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
