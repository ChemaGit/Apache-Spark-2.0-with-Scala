package exercises_cert

/** Question 8
  * Problem Scenario 63 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  * val b = a.map(x => (x.length, x))
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(Int, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))
  */

import org.apache.spark.sql._

object exercise_6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 6").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    b.reduceByKey({case(v1, v2) => v1 + v2}).collect.foreach(println)

    // res10: Array[(Int, String)] = Array((4,lion), (3,dogcat), (7,panther), (5,tigereagle))

    sc.stop()
    spark.stop()
  }
}
