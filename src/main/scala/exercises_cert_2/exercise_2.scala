package exercises_cert_2

import org.apache.spark.sql._

object exercise_2 {

  /** Question 38
    * Problem Scenario GG : You have been given below code snippet.
    * val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    * val b = a.keyBy(_.length)
    * val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
    * val d = c.keyBy(_.length)
    * operation1
    * Write a correct code snippet for operation1 which will produce desired output, shown below.
    * Array[(Int, String)] = Array((4,lion))
    */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 2").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("ant", "falcon", "squid"), 2)
    val d = c.keyBy(_.length)
    val e = b.subtractByKey(d)
    e.foreach(println)

    sc.stop()
    spark.stop()
  }

}
