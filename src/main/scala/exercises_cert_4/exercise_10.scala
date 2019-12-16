package exercises_cert_4

import org.apache.spark.sql.SparkSession

/** Question 75
  * Problem Scenario 54 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
  * val b = a.map(x => (x.length, x))
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(Int, String)] = Array((4,lion), (7,panther), (3,dogcat), (5,tigereagle))
  */

object exercise_10 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 10")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"))
    val b = a.map(x => (x.length, x))

    val operation = b
        .reduceByKey( (v, c) => s"$v$c")

    operation.foreach(println)

    sc.stop()
    spark.stop()
  }

}
