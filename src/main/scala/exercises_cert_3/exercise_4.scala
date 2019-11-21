package exercises_cert_3

import org.apache.spark.sql.SparkSession

/** Question 53
  * Problem Scenario 69 : Write down a Spark Application using Scala,
  * In which it read a file "Content.txt" (On hdfs) with following content.
  * And filter out the word which is less than 3 characters and ignore all empty lines.
  * Once done store the filtered data in a directory called "question53" (On hdfs)
  * Content.txt
  * Hello this is ABCTECH.com
  * This is ABYTECH.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  */

object exercise_4 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 4")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val filt = List("", " ")

    val content = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/Content.txt")
        .flatMap(line => line.split(" "))
        .filter(w => !filt.contains(w))
        .filter(w => w.length > 2)

    content.collect.foreach(r => println(r.mkString("")))

    content.repartition(1).saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercise_4")

    // check the results
    // hdfs dfs -ls /user/cloudera/exercise_4
    // hdfs dfs -cat /user/cloudera/exercise_4/part-00000

    sc.stop()
    spark.stop()
  }

}
