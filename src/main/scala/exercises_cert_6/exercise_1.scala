package exercises_cert_6

import org.apache.spark.sql.SparkSession

/** Question 95
  * Problem Scenario 59 : You have been given below code snippet.
  * val x = sc.parallelize(1 to 20)
  * val y = sc.parallelize(10 to 30)
  * operation1
  * z.collect
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[Int] = Array(16,12, 20,13,17,14,18,10,19,15,11)
  */

object exercise_1 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 1")
    .master("local[*]")
    .getOrCreate()
  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    val x = sc.parallelize(1 to 20)
    val y = sc.parallelize(10 to 30)

    val result = x.intersection(y)

    result.collect.foreach(println)

    sc.stop()
    spark.stop()
  }

}
