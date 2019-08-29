package exercises_cert

/** Question 6
  * Problem Scenario 65 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
  * val b = sc.parallelize(1 to a.count.toInt, 2)
  * val c = a.zip(b)
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))
  */

import org.apache.spark.sql._

object exercise_5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 5").master("local").getOrCreate()
    val sc = spark.sparkContext

    val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val b = sc.parallelize(1 to a.count.toInt, 2)
    val c = a.zip(b)
    c.sortByKey(false).collect.foreach(println)

    // res1: Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))

    spark.stop()
    sc.stop()
  }
}
