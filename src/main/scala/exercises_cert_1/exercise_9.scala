package exercises_cert_1

import org.apache.spark.sql._

/** Question 32
  * Problem Scenario 39 : You have been given two files
  * spark16/file1.txt
  * 1,9,5
  * 2,7,4
  * 3,8,3
  * spark16/file2.txt
  * 1,g,h
  * 2,i,j
  * 3,k,l
  * Load these two tiles as Spark RDD and join them to produce the below results
  * (1,((9,5),(g,h))) (2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
  * And write code snippet which will sum the second columns of above joined results (5+4+3).
  */

object exercise_9 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 9").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val file1 = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/file11.txt").map(line => line.split(",")).map(arr => (arr(0).toInt,(arr(1).toInt,arr(2).toInt)))
    val file2 = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/file22.txt").map(line => line.split(",")).map(arr => (arr(0).toInt,(arr(1),arr(2))))
    val joined = file1.join(file2)
    val result = joined.map({case( (k,((v,c),(m,n)))) => c}).reduce({case(c, c1) => c + c1})
    println(result)

    sc.stop()
    spark.stop()
  }
}
