package advanced_spark_programming

import org.apache.spark.sql.SparkSession

object BroadcastVariables {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Broadcast Variables").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val broadcast_var = sc.broadcast(List(1,2,3))
		println(broadcast_var.value.mkString(","))

		val names = sc.parallelize(List(("www.google.com","Google"),("www.yahoo.in","Yahoo")))
		val visits = sc.parallelize(List(("www.google.com",90),("www.yahoo.in",10)))
		val pageMap = names.collect.toMap
		val bcMap = sc.broadcast(pageMap)

		val joined = visits.map{case(url,counts) => (url,(bcMap.value(url),counts))}
		joined.collect.foreach(println)

		sc.stop()
		spark.stop()
	}
}
