package  exercises_cert_1

/** Question 23
  * Problem Scenario 60 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  * val b = a.keyBy(_.length)
  * val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
  * val d = c.keyBy(_.length)
  * operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(Int, (String, String))] = Array((6,(salmon,salmon)), (6,(salmon,rabbit)),(6,(salmon,turkey)), (6,(salmon,salmon)),
  * (6,(salmon,rabbit)),(6,(salmon,turkey)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)),(3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))
  */

import org.apache.spark.sql._

object exercise_6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 6").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val d = c.keyBy(_.length)
    b.join(d).collect.foreach(println)

    // res1: Array[(Int, (String, String))] = Array((6,(salmon,salmon)), (6,(salmon,rabbit)), (6,(salmon,turkey)), (6,(salmon,salmon)),
    // (6,(salmon,rabbit)), (6,(salmon,turkey)), (3,(dog,dog)), (3,(dog,cat)), (3,(dog,gnu)), (3,(dog,bee)), (3,(rat,dog)), (3,(rat,cat)), (3,(rat,gnu)), (3,(rat,bee)))

    sc.stop()
    spark.stop()
  }

}

