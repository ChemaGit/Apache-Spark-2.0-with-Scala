package exercises_cert_3

import org.apache.spark.sql.SparkSession

object exercise_8 {

  /** Question 57
    * Problem Scenario 73 : You have been given data in json format as below.
    * {"first_name":"Ankit", "last_name":"Jain"}
    * {"first_name":"Amir", "last_name":"Khan"}
    * {"first_name":"Rajesh", "last_name":"Khanna"}
    * {"first_name":"Priynka", "last_name":"Chopra"}
    * {"first_name":"Kareena", "last_name":"Kapoor"}
    * {"first_name":"Lokesh", "last_name":"Yadav"}
    * Do the following activity
    * 1. create employee.json file locally.
    * 2. Load this file on hdfs
    * 3. Register this data as a temp table in Spark using Scala.
    * 4. Write select query and print this data.
    * 5. Now save back this selected data in json format on the directory question57.
    */

  def main(args: Array[String]): Unit = {
    /**
      * build the file locally and put it in HDFS
      * $ gedit /home/cloudera/files/employee.json &
      * $ hdfs dfs -put /home/cloudera/files/employee.json /user/cloudera/files/
      * $ hdfs dfs -cat /user/cloudera/files/employee.json
      */
    val spark = SparkSession
      .builder()
      .appName("exercise 8")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val emp = spark.sqlContext.read.json("hdfs://quickstart.cloudera/user/cloudera/files/employee.json")
    emp.createOrReplaceTempView("employee")
    spark.sqlContext.sql("""SELECT date_format(current_date,'dd/MM/yyyy') AS date, first_name, last_name, concat(first_name,",",last_name) FROM employee""").show()

    val result = spark.sqlContext.sql("""SELECT date_format(current_date,'dd/MM/yyyy') AS date, first_name, last_name, concat(first_name,",",last_name) FROM employee""")
    result.toJSON.repartition(1).rdd.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/question_57")

    /**
      * Check the results
      * $ hdfs dfs -ls /user/cloudera/question_57/
      * $ hdfs dfs -cat /user/cloudera/question_57/part-00000
      */

    sc.stop()
    spark.stop()

  }

}