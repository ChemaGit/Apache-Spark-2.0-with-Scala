package exercises_cert_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/** Question 22
  * Problem Scenario 41 : You have been given below code snippet.
  * val au1 = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
  * val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))
  * Apply the Spark method, which will generate below output.
  * Array[(String, Array[Int])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a(Array(3)), (b,Array(2)))
  */

object exercise_5 {

  val spark = SparkSession
    .builder()
    .appName("exercise_5")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_5")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    try {

      Logger.getRootLogger.setLevel(Level.ERROR)

      val au1 = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
      val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))

      au1
        .union(au2)
        .collect
        .foreach(x => println(x._1 + "," + x._2.mkString(",")))

      // res0: Array[(String, Array[Int])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a,Array(3)), (b,Array(2)))
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
