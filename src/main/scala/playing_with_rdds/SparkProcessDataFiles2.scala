package playing_with_rdds

import org.apache.spark.sql._

// 2014-03-15:10:10:20,Ronin Novelty Note 2,a678ccc3-b0d2-452d-bf89-85bd095e28ee,0,10,97,63,enabled,enabled,connected,48,4,32.2850556785,-111.819583734
// 2014-03-15:10:10:20|MeeToo 4.1|673f7e4b-d52b-44fc-8826-aea460c3481a|1|16|77|61|24|50|enabled|disabled|enabled|34.1841062345|-117.9435329

object SparkProcessDataFiles2 {
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("Spark Process Data Files").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		
		val data = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/devicestatus.txt")
		val parse = data.map(line =>{
			if(line.substring(18,18) == ",") line.split(',')
			else line.split('|')
		} )
		val filtered = parse.filter(arr => arr.length == 14)
		val extract = filtered.map(arr => "%s-%s-%s-%s-%s".format(arr(0),arr(1),arr(2),arr(12),arr(13)))
		//extract.take(10).foreach(println)
		extract.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/results/devicestatus_etl")

		sc.stop
		spark.stop
	}
}
