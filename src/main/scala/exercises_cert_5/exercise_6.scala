package exercises_cert_5

/** Question 86
  * Problem Scenario 44 : You have been given 4 files , with the content as given below:
  * spark11/file_1.txt
  * Apache Hadoop is an open-source software framework written in Java for distributed
  * storage and distributed processing of very large data sets on computer clusters built from
  * commodity hardware. All the modules in Hadoop are designed with a fundamental
  * assumption that hardware failures are common and should be automatically handled by the framework
  * spark11/file_2.txt
  * The core of Apache Hadoop consists of a storage part known as Hadoop Distributed File
  * System (HDFS) and a processing part called MapReduce. Hadoop splits files into large
  * blocks and distributes them across nodes in a cluster. To process data, Hadoop transfers
  * packaged code for nodes to process in parallel based on the data that needs to be processed.
  * spark11/file_3.txt
  * his approach takes advantage of data locality nodes manipulating the data they have
  * access to to allow the dataset to be processed faster and more efficiently than it would be
  * in a more conventional supercomputer architecture that relies on a parallel file system
  * where computation and data are distributed via high-speed networking
  * spark11/file_4.txt
  * Apache Storm is focused on stream processing or what some call complex event
  * processing. Storm implements a fault tolerant method for performing a computation or
  * pipelining multiple computations on an event as it flows into a system. One might use
  * Storm to transform unstructured data as it flows into a system into a desired format
  * (spark11/file_1.txt)
  * (spark11/file_2.txt)
  * (spark11/file_3.txt)
  * (spark11/file_4.txt)
  * Write a Spark program, which will give you the highest occurring words in each file. With their file name and highest occurring words.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object exercise_6 {

  val spark = SparkSession
    .builder()
    .appName("exercise_6")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_6")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  val output = "hdfs://quickstart.cloudera/user/cloudera/exercises/question_86"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val n1 = "file_1.txt ==> "
      val n2 = "file_2.txt ==> "
      val n3 = "file_3.txt ==> "
      val n4 = "file_4.txt ==> "

      val file1 = sc
        .textFile(s"${path}file_1.txt")
        .flatMap(line => line.split("\\W"))
        .filter(w => !w.isEmpty)
        .map(w => (w, 1))
        .reduceByKey( (v,c) => v + c)
        .sortBy(t => t._2, false)

      val file2 = sc
        .textFile(s"${path}file_2.txt")
        .flatMap(line => line.split("\\W"))
        .filter(w => !w.isEmpty)
        .map(w => (w, 1))
        .reduceByKey( (v,c) => v + c)
        .sortBy(t => t._2, false)

      val file3 = sc
        .textFile(s"${path}file_3.txt")
        .flatMap(line => line.split("\\W"))
        .filter(w => !w.isEmpty)
        .map(w => (w, 1))
        .reduceByKey( (v,c) => v + c)
        .sortBy(t => t._2, false)

      val file4 = sc
        .textFile(s"${path}file_4.txt")
        .flatMap(line => line.split("\\W"))
        .filter(w => !w.isEmpty)
        .map(w => (w, 1))
        .reduceByKey( (v,c) => v + c)
        .sortBy(t => t._2, false)

      val lHighestWords = sc
        .parallelize(List( (n1,file1.first()), (n2,file2.first()), (n3,file3.first()), (n4,file4.first()) ))
        .saveAsTextFile(output)

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}
