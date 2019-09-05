package playing_with_rdds

import org.apache.spark.sql._

object SparkBasics {

	def toUpper(s: String): String = s.toUpperCase

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Spark Basics").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val mydata = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/purplecow.txt")
		mydata.collect.foreach(x => println(x))
		println()
		println(s"mydata.count: ${mydata.count}") // count() is an action

		//map and filter transformations
		println()
		val mapRdd = mydata.map(line => line.toUpperCase)
		mapRdd.collect.foreach(line => println(line))
		println()
		val filterRdd = mapRdd.filter(line => line.startsWith("I"))
		filterRdd.collect.foreach(line => println(line))
		println()
		println(s"filterRdd.count: ${filterRdd.count}")
		println()
		println(filterRdd.toDebugString)
		println()
		val upper = mydata.map(toUpper)
		upper.collect.foreach(println)
		println()
		println("************************************************")
		println()
		val data = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/frostroad.txt")
		println(s"data.count: ${data.count}")
		println()
		data.collect.foreach(println)
		println()
		println("*************************************************")
		val logfiles = "hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/*"
		val logsRDD = sc.textFile(logfiles)
		val jpglogsRDD = logsRDD.filter(line => line.contains(".jpg"))
		logsRDD.take(10).foreach(println)
		println("****************************************************")
		println("Number of lines: " + sc.textFile(logfiles).filter(line => line.contains(".jpg")).count())
		println("****************************************************")
		println("Length of lines: " + logsRDD.map(line => line.length).take(5).foreach(println))		
		val rdd = logsRDD.map(line => line.split(" "))
		rdd.take(10).foreach(arr => println(arr.mkString(",")))
		println("*****************************************************")
		val ipsRDD = rdd.map(arr => arr(0))
		ipsRDD.take(10).foreach(println)

		ipsRDD.repartition(1).saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/results/spark_basics")
		println("******************************************************")
		val ipUser = rdd.map(arr => "%s/%s".format(arr(0),arr(2)))
		ipUser.take(10).foreach(println)

		sc.stop
		spark.stop
	}
}
