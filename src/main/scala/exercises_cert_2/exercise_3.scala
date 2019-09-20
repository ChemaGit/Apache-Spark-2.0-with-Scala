package exercises_cert_2

import org.apache.spark.sql._
/** Question 41
  * Problem Scenario 70 : Write down a Spark Application using Scala, In which it read a
  * file "content.txt" (On hdfs) with following content. Do the word count and save the
  * results in a directory called "problem85" (On hdfs)
  * content.txt
  * Hello this is ABCTECH.com
  * This is XYZTECH.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  */
object exercise_3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 3").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val file = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/Content.txt")
    val wordCount = file.flatMap(line => line.split("\\W")).map(w => (w, 1)).reduceByKey( (v, v1) => v + v1).sortBy(t => t._2, false)
    wordCount.foreach(println)
    wordCount.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/question_41")

    sc.stop()
    spark.stop()
  }

}
