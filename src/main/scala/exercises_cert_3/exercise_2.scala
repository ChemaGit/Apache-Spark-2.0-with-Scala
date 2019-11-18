package exercises_cert_3

import org.apache.spark.sql.SparkSession

/** Question 51
  * Problem Scenario 72 : You have been given a table named "employee2" with following detail.
  * first_name string
  * last_name string
  * Write a spark script in scala which read this table and print all the rows and individual column values.
  * employee.json
  * {"first_name":"Ankit", "last_name":"Jain"}
  * {"first_name":"Amir", "last_name":"Khan"}
  * {"first_name":"Rajesh", "last_name":"Khanna"}
  * {"first_name":"Priynka", "last_name":"Chopra"}
  * {"first_name":"Kareena", "last_name":"Kapoor"}
  * {"first_name":"Lokesh", "last_name":"Yadav"}
  */

object exercise_2 {

  // edit the file
  // $ gedit employee.json
  // put the file in hadoop file system
  // $ hdfs dfs -put employee.json /user/cloudera/files/

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 2")
      .master("local[2]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","hdfs://quickstart.cloudera/user/hive/warehouse/")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    sc.getConf.getAll.foreach(println)

    // create database hadoopexam
    spark.sqlContext.sql("""CREATE DATABASE IF NOT EXISTS hadoopexam LOCATION "hdfs://quickstart.cloudera/user/hive/warehouse/" """)
    // spark.sqlContext.sql("""show databases""").show()
    // spark.sqlContext.sql("""show tables""").show()
    // read the file and create a table employee2 whit Spark
    val employee = spark.sqlContext.read.json("hdfs://quickstart.cloudera/user/cloudera/files/employee.json")
    employee.repartition(1)
      .rdd
      .map(r => r.mkString(","))
        .saveAsTextFile("hdfs://quickstart.cloudera/user/hive/warehouse/hadoopexam.db/t_employee2")

    spark.sqlContext.sql("""use hadoopexam""")
    spark.sqlContext.sql("""CREATE TABLE IF NOT EXISTS t_employee2(first_name string, last_name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LOCATION "hdfs://quickstart.cloudera/user/hive/warehouse/hadoopexam.db/t_employee2" """)

    spark.sqlContext.sql("""SELECT * FROM t_employee2""").show()

    sc.stop()
    spark.stop()
  }
}
