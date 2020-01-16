package exercises_cert_5

import org.apache.spark.sql.SparkSession

/** Question 93
  * Problem Scenario 36 : You have been given a file named /home/cloudera/files/data.csv (type,name).
  * data.csv
  * 1,Lokesh
  * 2,Bhupesh
  * 2,Amit
  * 2,Ratan
  * 2,Dinesh
  * 1,Pavan
  * 1,Tejas
  * 2,Sheela
  * 1,Kumar
  * 1,Venkat
  * 1. Load this file from hdfs and save it back as (id, (all names of same type)) in results directory. However, make sure while saving it should be only one file.
  *
  * $ gedit /home/cloudera/files/data.csv
  * $ hdfs dfs -put /home/cloudera/files/data.csv /user/cloudera/files
  * $ hdfs dfs -cat /user/cloudera/files/data.csv
  */

object exercise_9 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 9")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    val data = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/data.csv")
        .map(line => line.split(","))
        .map(arr => (arr(0), arr(1)))
        .groupByKey()
        .map({case(id, names) => (id, "(%s)".format(names.mkString(",")))})
        .repartition(1)
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercises/question_93")

    // $ hdfs dfs -ls /user/cloudera/exercises/question_93
    // $ hdfs dfs -cat /user/cloudera/exercises/question_93/part-00000

    sc.stop()
    spark.stop()
  }

}
