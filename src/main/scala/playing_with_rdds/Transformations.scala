package playing_with_rdds

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Transformations {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("mapExample").master("local").getOrCreate()

		spark.sparkContext.setLogLevel("ERROR")
		// map function
		println("*******map function************")
		val data = spark.read.textFile("hdfs://quickstart.cloudera/user/cloudera/files/file_4.txt").rdd
		val mapFile = data.map(line => (line, line.length))
		mapFile.collect.foreach(println)
		
		// flatMap function
		println("*******flatMap function************")
		val flatMapFile = data.flatMap(lines => lines.split(" "))
		flatMapFile.collect.foreach(println)

		// filter function
		println("*******filter function************")
		val filterFile = data.flatMap(lines => lines.split(" ")).filter(value => value == "Apache")
		println("Number of lines: " + filterFile.count)
		filterFile.collect.foreach(println)

		spark.stop()
	}
}
