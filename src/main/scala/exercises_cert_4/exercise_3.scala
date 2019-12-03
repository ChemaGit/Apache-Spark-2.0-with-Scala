package exercises_cert_4

import org.apache.spark.sql.SparkSession


/** Question 65
  * Problem Scenario 91 : You have been given data in json format as below.
  * {"first_name":"Ankit", "last_name":"Jain"}
  * {"first_name":"Amir", "last_name":"Khan"}
  * {"first_name":"Rajesh", "last_name":"Khanna"}
  * {"first_name":"Priynka", "last_name":"Chopra"}
  * {"first_name":"Kareena", "last_name":"Kapoor"}
  * {"first_name":"Lokesh", "last_name":"Yadav"}
  * Do the following activity
  * 1. create employee.json file locally.
  * 2. Load this file on hdfs
  * 3. Register this data as a temp table in Spark using scala.
  * 4. Write select query and print this data.
  * 5. Now save back this selected data as table in format orc. /user/cloudera/question65/orc
  * 6. Now save back this selected data as parquet file compressed in snappy codec in HDFS /user/cloudera/question65/parquet-snappy
  *
  * Build the file and put it in HDFS file system
  * $ gedit /home/cloudera/files/employee.json
  * $ hdfs dfs -put /home/cloudera/files/employee.json /user/cloudera/files/
  */
object exercise_3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 3")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val employee = spark
      .sqlContext
      .read
      .json("hdfs://quickstart.cloudera/user/cloudera/files/employee.json")

    employee.createOrReplaceTempView("employee")

    spark
      .sqlContext
      .sql("""SELECT first_name, last_name, CONCAT(first_name,", ", last_name) AS full_name FROM employee""")
      .show()

    employee
      .repartition(1)
      .write
      .orc("hdfs://quickstart.cloudera/user/cloudera/exercise_3/orc")

    spark
      .sqlContext
        .setConf("spark.sql.parquet.compression.codec","snappy")

    employee
        .repartition(1)
        .write
        .parquet("hdfs://quickstart.cloudera/user/cloudera/parquet-snappy")

    sc.stop()
    spark.stop()
  }

  /**
    * Check the files
    * $ hdfs dfs -ls /user/cloudera/exercise_3/orc
    * $ hdfs dfs -ls /user/cloudera/exercise_3/parquet-snappy
    * $ parquet-tools meta hdfs://quickstart.cloudera/user/cloudera/question65/parquet-snappy/part-r-00000-9c6e9286-8d47-4274-9391-b26bdc66f3bb.snappy.parquet
    * $ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/question65/parquet-snappy/part-r-00000-9c6e9286-8d47-4274-9391-b26bdc66f3bb.snappy.parquet
    */
}
