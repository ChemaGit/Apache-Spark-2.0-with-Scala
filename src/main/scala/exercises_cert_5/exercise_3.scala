package exercises_cert_5

import org.apache.spark.sql.SparkSession


/** Question 78
  * Problem Scenario 35 : You have been given a file named spark7/EmployeeName.csv (id,name).
  * EmployeeName.csv
  * E01,Lokesh
  * E02,Bhupesh
  * E03,Amit
  * E04,Ratan
  * E05,Dinesh
  * E06,Pavan
  * E07,Tejas
  * E08,Sheela
  * E09,Kumar
  * E10,Venkat
  * 1. Load this file from hdfs and sort it by name and save it back as (id,name) in results directory. However, make sure while saving it should be able to write In a single file.
  *
  * $ gedit /home/cloudera/files/EmployeeName.csv
  * $ hdfs dfs -put /home/cloudera/files/EmployeeName.csv /user/cloudera/files/
  */
object exercise_3 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 3")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    val emp = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/EmployeeName.csv")
        .map(lines => lines.split(","))
        .map(r => (r(0), r(1)))
        .sortBy(t => t._2)

    emp.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercises/question_78/")

    /**
      * $ hdfs dfs -ls /user/cloudera/exercises/question_78/
      * $ hdfs dfs -cat /user/cloudera/exercises/question_78/part*
      */

    sc.stop()
    spark.stop()
  }

}
