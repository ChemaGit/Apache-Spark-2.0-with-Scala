package exercises_cert_1

import org.apache.spark.sql._

/** Question 29
  * Problem Scenario 61 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  * val b = a.keyBy(_.length)
  * val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
  * val d = c.keyBy(_.length)
  * operation1
  * Write a correct code snippet for operationl which will produce desired output, shown below.
  * Array[(Int, (String, Option[String]))] = Array((6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),(6,(salmon,Some(turkey))), (6,(salmon,Some(salmon))), (6,(salmon,Some(rabbit))),
  *                                                (6,(salmon,Some(turkey))), (3,(dog,Some(dog))), (3,(dog,Some(cat))),(3,(dog,Some(dog))), (3,(dog,Some(bee))), (3,(rat,Some(dogg)), (3,(rat,Some(cat))),
  *                                                (3,(rat.Some(gnu))). (3,(rat,Some(bee))), (8,(elephant,None)))
  */

object exercise_8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 8").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val d = c.keyBy(_.length)

    val result = b.leftOuterJoin(d)
    result.foreach(x => println(x))

    sc.stop()
    spark.stop()
  }
}
