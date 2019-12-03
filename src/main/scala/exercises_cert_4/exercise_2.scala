package exercises_cert_4

import org.apache.spark.sql.SparkSession


/** Question 64
  * Problem Scenario 34 : You have given a file named spark6/user.csv.
  * Data is given below:
  * user.csv
  * id,topic,hits
  * Rahul,scala,120
  * Nikita,spark,80
  * Mithun,spark,1
  * myself,cca175,180
  * Now write a Spark code in scala which will remove the header part and create RDD of values as below, for all rows. And also if id is myself" than filter out row.
  * Map(id -> om, topic -> scala, hits -> 120)
  *
  * Build the file and put it in HDFS file system
  * $ gedit /home/cloudera/files/user.csv
  * $ hdfs dfs -put /home/cloudera/files/user.csv /user/cloudera/files
  */
object exercise_2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 2")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val users = sc
      .textFile("hdfs://quickstart.cloudera/user/cloudera/files/user.csv")
      .map(line => line.split(","))
    val head = users.first()
    val wFilter = List(head(0), "myself")
    val userFiltered = users
        .filter(r => !wFilter.contains(r(0)))
        .map(r => r.zip(head).toMap)

    userFiltered.collect.foreach(println)

    sc.stop()
    spark.stop()
  }

}
