package exercises_cert_4

import org.apache.spark.sql.SparkSession


/** Question 63
  * Problem Scenario 57 : You have been given below code snippet.
  * val a = sc.parallelize(1 to 9, 3) operation1
  * Write a correct code snippet for operation1 which will produce desired output, shown below.
  * Array[(String, Seq[Int])] = Array((even,ArrayBuffer(2, 4, 6, 8)), (odd,ArrayBuffer(1, 3, 5, 7,9)))
  */
object exercise_1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 1")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(1 to 9, 3)
    val operation = a.groupBy(v => if(v % 2 == 0) "even" else "odd")
    operation.collect.foreach(v => println(s"key: ${v._1} -- value: ${v._2.mkString(",")}"))

    sc.stop()
    spark.stop()
  }

}
