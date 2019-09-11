package playing_with_rdds

import org.apache.spark.sql._

object JoinTwoDatasets {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Join Two Datasets").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val webLogs = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/*2.log")
		val wMap = webLogs.map(line => line.split(" ")).map(arr => (arr(2).toInt,1))
		// wMap.take(10).foreach(println)
		val wReduce = wMap.reduceByKey( (v, v1) => v + v1)
		val wFrequency = wReduce.map(pair => pair.swap).countByKey
		//wFrequency.foreach(println)

		val ipUser = webLogs.map(line => line.split(" ")).map(arr => (arr(2),arr(0))).groupByKey
		//ipUser.take(10).foreach(println)

		// Join two datasets
		val accounts = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/accounts").map(line => line.split(",")).map(arr => (arr(0).toInt, arr))
		val joined = accounts.join(wReduce)
		// joined.take(5).foreach(println)
		val result = joined.map({case(u,(arr,n)) => "%d %d %s %s".format(u,n,arr(3), arr(4))})
		result.take(10).foreach(println)
		/****************/
		println()

		val accounts2 = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/accounts").map(line => line.split(",")).keyBy(arr => arr(8).toInt).groupByKey()
		// accounts2.take(10).foreach(println)
		val result2 = accounts2.flatMapValues(iter => iter.map(arr => "%s %s".format(arr(3), arr(4)))).groupByKey().sortByKey()
		result2.take(5).foreach({case(pc, iter) =>
			println(s"---$pc")
			iter.foreach(n => println(s"\t$n"))
			println("***")})

		sc.stop()
		spark.stop()
	}
}
