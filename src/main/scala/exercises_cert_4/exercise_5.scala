package exercises_cert_4

import org.apache.spark.sql.SparkSession

/** Question 69
  * Problem Scenario 64 : You have been given below code snippet.
  * val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
  * val b = a.keyBy(_.length)
  * val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
  * val d = c.keyBy(_.length)
  * operation1
  * Write a correct code snippet for operationl which will produce desired output, shown below.
  * Array[(Int, (Option[String], String))] = Array((6,(Some(salmon),salmon)),
  * (6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), (6,(Some(salmon),salmon)),
  * (6,(Some(salmon),rabbit)), (6,(Some(salmon),turkey)), (3,(Some(dog),dog)),
  * (3,(Some(dog),cat)), (3,(Some(dog),gnu)), (3,(Some(dog),bee)), (3,(Some(rat),
  * (3,(Some(rat),cat)), (3,(Some(rat),gnu)), (3,(Some(rat),bee)), (4,(None,wolf)),(4,(None,bear)))
  */

object exercise_5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 5")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    val b = a.keyBy(_.length)
    val c = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val d = c.keyBy(_.length)

    val operation = b.rightOuterJoin(d)
    operation.foreach(println)

    sc.stop()
    spark.stop()
  }
}