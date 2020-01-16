package exercises_cert_6

import org.apache.spark.sql.SparkSession

/** Question 96
  * Problem Scenario 62 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  * val b = a.map(x => (x.length, x))
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(Int, String)] = Array((3,xdogx), (5,xtigerx), (4,xlionx), (3,xcatx), (7,xpantherx),(5,xeaglex))
  */

object exercise_2 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 2")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    val operation1 = b.mapValues(v => s"x${v}x")
    operation1.foreach(println)

    sc.stop()
    spark.stop()
  }

}
