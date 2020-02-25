package exercises_cert

/** Question 5
  * Problem Scenario 53 : You have been given below code snippet.
  * val a = sc.parallelize(1 to 10, 3)
  * operation1 b.collect
  * Output 1
  * Array[Int] = Array(2, 4, 6, 8,10)
  * operation2
  * Output 2
  * Array[Int] = Array(1,2, 3)
  * Write a correct code snippet for operation1 and operation2 which will produce desired output, shown above.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object exercise_4 {

  val spark = SparkSession
    .builder()
    .appName("exercise 4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_4")  // To silence Metrics warning
    .getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val a = sc.parallelize(1 to 10, 3)
      val b = a.filter(v => v % 2 == 0)
      b.collect.foreach(println)
      // res2: Array[Int] = Array(2, 4, 6, 8, 10)


      val c = a.filter(v => v <= 3)
      c.collect.foreach(println)
      // res3: Array[Int] = Array(1, 2, 3)

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
