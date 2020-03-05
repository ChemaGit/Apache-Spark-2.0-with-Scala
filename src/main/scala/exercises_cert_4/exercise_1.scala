package exercises_cert_4

/** Question 63
  * Problem Scenario 57 : You have been given below code snippet.
  * val a = sc.parallelize(1 to 9, 3) operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(String, Seq[Int])] = Array((even,ArrayBuffer(2, 4, 6, 8)), (odd,ArrayBuffer(1, 3, 5, 7,9)))
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object exercise_1 {

  val spark = SparkSession
    .builder()
    .appName("exercise_1")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_1")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val a = sc
        .parallelize(1 to 9, 3)
        .groupBy(v => if(v % 2 == 0) "even" else "odd")
        .cache()

      a
        .collect
        .foreach(v => println(s"key: ${v._1} -- value: ${v._2.mkString(",")}"))

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
