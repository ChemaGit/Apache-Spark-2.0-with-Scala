package exercises_cert_4

import org.apache.spark.sql.SparkSession

/** Question 73
  * Problem Scenario 71 :
  * Write down a Spark script using Scala,
  * In which it read a file "Content.txt" (On hdfs) with following content.
  * After that split each row as (key, value), where key is first word in line and entire line as value.
  * Filter out the empty lines.
  * And save this key value in "question73" as Sequence file(On hdfs)
  * Part 2 : Save as sequence file , where key as null and entire line as value. Read back the stored sequence files.
  * Content.txt
  * Hello this is ABCTECH.com
  * This is XYZTECH.com
  * Apache Spark Training
  * This is Spark Learning Session
  * Spark is faster than MapReduce
  *
  * Create the file and put it into HDFS
  * $ gedit /home/cloudera/files/Content.txt
  * $ hdfs dfs -put /home/cloudera/files/Content.txt /user/cloudera/files
  */

object exercise_8 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise_8")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    val content = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/Content.txt")
        .map(line => (line.split(" ")(0), line))
        .saveAsSequenceFile("hdfs://quickstart.cloudera/user/cloudera/exercises/question_73/")

    val sequence = sc
        .sequenceFile("hdfs://quickstart.cloudera/user/cloudera/exercises/question_73/",classOf[org.apache.hadoop.io.Text], classOf[org.apache.hadoop.io.Text])

    val printSequence = sequence
        .map(t => (t._1.toString, t._2.toString))
        .collect
        .foreach(println)

    sc.stop()
    spark.stop()
  }

}
