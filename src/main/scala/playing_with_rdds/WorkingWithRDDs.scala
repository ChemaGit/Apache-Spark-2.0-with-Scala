package playing_with_rdds

import org.apache.spark.sql._
import scala.util.parsing.json.JSON

object WorkingWithRDDs {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Working with RDDs").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		// Creating RDDs from collections
		val myData = List("Alice","Carlos","Frank","Barbara")
		val myRdd = sc.parallelize(myData)
		myRdd.collect.foreach(println)

		// Creating RDDs from Text Files
		println("****************************************")
		val dataFile = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/purplecow.txt")
		dataFile.collect.foreach(println)

		println("*****************************************")
		val files = "hdfs://quickstart.cloudera/user/cloudera/loudacre/data/purplecow.txt,hdfs://quickstart.cloudera/user/cloudera/loudacre/data/frostroad.txt"
		val dataFiles = sc.textFile(files)
		dataFiles.collect.foreach(println)
		println("*****************************************")
		println("Using wholeTextFiles")
		println()
		val myRdd1 = sc.wholeTextFiles("hdfs://quickstart.cloudera/user/cloudera/loudacre/json_files")
		val myRdd2 = myRdd1.map(pair => pair._2)
		val myRdd3 = myRdd2.map(line => JSON.parseFull(line).get.asInstanceOf[Map[String,String]])
		for(record <- myRdd3.take(2)) println(record.getOrElse("first_name",null))

		println("**************************************")
		println("Using flatMap and distinct")
		val file = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/frostroad.txt")
		val res = file.flatMap(line => line.split(" ")).distinct()
		res.collect.foreach(println)

		println("*****************************************")
		println("Multi RDD transformation")
		val rddl1 = sc.parallelize(List("Chicago","Boston","Paris","San Francisco","Tokyo"))
		val rddl2 = sc.parallelize(List("San Francisco","Boston","Amsterdam","Mumbai","McMurdo Station"))

		val substract = rddl1.subtract(rddl2)
		substract.collect.foreach(println)
		println()
		val zip = rddl1.zip(rddl2)
		zip.collect.foreach(println)
		println()
		val union = rddl1.union(rddl2)
		union.collect.foreach(println)
		println()
		val intersection = rddl1.intersection(rddl2)
		intersection.collect.foreach(println)
		sc.stop()
		spark.stop()
	}
}
