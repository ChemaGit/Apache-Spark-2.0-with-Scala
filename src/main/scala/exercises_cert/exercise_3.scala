package exercises_cert

/**
  * /** Question 4
  * * Problem Scenario 58 : You have been given below code snippet.
  * * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
  * * val b = a.keyBy(_.length)
  * * operation1
  * * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * * Array[(Int, Seq[String])] = Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)),(3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle)))
  **/
  */

import org.apache.spark.sql._

object exercise_3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 3").master("local").getOrCreate()
    val sc = spark.sparkContext

    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val b = a.keyBy(_.length)
    val c = b.groupByKey.collect.foreach(x => println(x._1 + ", " + x._2.mkString(",")))
    // res1: Array[(Int, Iterable[String])] = Array((4,CompactBuffer(lion)), (6,CompactBuffer(spider)), (3,CompactBuffer(dog, cat)), (5,CompactBuffer(tiger, eagle)))

    sc.stop()
    spark.stop()
  }
}
