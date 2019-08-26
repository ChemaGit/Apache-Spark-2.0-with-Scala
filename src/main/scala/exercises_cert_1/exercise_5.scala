package exercises_cert_1

import org.apache.spark.sql.SparkSession

/** Question 22
  * Problem Scenario 41 : You have been given below code snippet.
  * val au1 = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
  * val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))
  * Apply the Spark method, which will generate below output.
  * Array[(String, Array[Int])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a(Array(3)), (b,Array(2)))
  */

object exercise_5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 5").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val au1 = sc.parallelize(List (("a" , Array(1,2)), ("b" , Array(1,2))))
    val au2 = sc.parallelize(List (("a" , Array(3)), ("b" , Array(2))))
    au1.union(au2).collect.foreach(x => println(x._1 + "," + x._2.mkString(",")))

    // res0: Array[(String, Array[Int])] = Array((a,Array(1, 2)), (b,Array(1, 2)), (a,Array(3)), (b,Array(2)))
    sc.stop()
    spark.stop()
  }
}
