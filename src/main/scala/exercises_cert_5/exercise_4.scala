package exercises_cert_5

import org.apache.spark.sql.SparkSession

/** Question 82
  * Problem Scenario 40 : You have been given sample data as below in a file called /home/cloudera/files/file222.txt
  * 3070811,1963,1096,,"US","CA",,1,
  * 3022811,1963,1096,,"US","CA",,1,56
  * 3033811,1963,1096,,"US","CA",,1,23
  * Below is the code snippet to process this tile.
  * val field= sc.textFile("spark15/file1.txt")
  * val mapper = field.map(x=> A)
  * mapper.map(x => x.map(x=> {B})).collect
  * Please fill in A and B so it can generate below final output
  * Array(Array(3070811,1963,109G, 0, "US", "CA", 0,1, 0),Array(3022811,1963,1096, 0, "US", "CA", 0,1, 56),Array(3033811,1963,1096, 0, "US", "CA", 0,1, 23)
  *
  * Create the files
  * $ gedit /home/cloudera/files/file222.txt &
  * $ hdfs dfs -put /home/cloudera/file222.txt /user/cloudera/files/
  */

object exercise_4 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 4")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    val field= sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/file222.txt")
    val mapper = field.map(x => x.split(","))
    val result = mapper
      .map(x => x.map(x => {if(x.isEmpty || x == "" || x == " ") 0 else x}))
      .collect

    result.foreach(x => println(x.mkString(",")))

    sc.stop()
    spark.stop()
  }
}
