package playing_with_rdds

import org.apache.spark.sql._

object AggregatingDataWithRdds {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Aggregating Data With Pair RDDs").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val users = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/users.txt").map(line => line.split(",")).map(fields => (fields(0),fields(1)))
		users.collect.foreach(println)

		// Keying Web Logs by User ID
		val logs = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/2014-03-15.log").keyBy(line => line.split(" ")(2))
		logs.take(10).foreach(println)
		
		val base = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/base_stations.tsv").map(line => line.split('\t')).map(fields => (fields(1),(fields(4),fields(5))))
		base.take(10).foreach(println)

		val skus = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/skus.txt").map(line => (line.split('\t')(0),line.split('\t')(1))).flatMapValues(v => v.split(":"))
		skus.collect.foreach(println)

		// Map reduce: example Word Count
		val cFile = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/frostroad.txt").flatMap(line => line.split("\\W")).map(w => (w, 1))
		val count = cFile.reduceByKey( (v,v1) => v + v1).sortBy(t => t._2, false)
		count.collect.foreach(println)

		sc.stop
		spark.stop
	}
}
