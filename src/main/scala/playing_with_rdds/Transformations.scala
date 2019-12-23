package playing_with_rdds

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

		// union function
		println("*******union function************")
		val sc = spark.sparkContext
		val rdd1 = sc.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))	
		val rdd2 = sc.parallelize(Seq((5,"dec",2014),(17,"sep",2015),(1,"jan",2016)))		
		val rdd3 = sc.parallelize(Seq((6,"dec",2016),(16,"may",2015)))	
		val unionRdd = rdd1.union(rdd2).union(rdd3)
		unionRdd.collect.foreach(println)

		// intersection function
		println("*******intersection function************")
		val common = rdd1.intersection(rdd2)
		common.collect.foreach(println)

		// distinct function
		println("**********distinct function*****************")
		val result = rdd1.distinct()
		result.collect.foreach(println)

		// groupByKey function		
		println("**********distinct function*****************")
		val mapData = data.flatMap(line => line.split(" ")).map(w => (w, 1))
		val group = mapData.groupByKey()
		group.collect.foreach(println)
		val wordCount = group.map(t => (t._1, t._2.size))
		wordCount.collect.foreach(println)

		//reduceByKey function
		println("**********reduceByKey function*****************")
		val reduceMap = mapData.reduceByKey({case(v, v1) => v + v1}).sortBy(t => t._2, false)
		reduceMap.collect.foreach(println)

		// sortByKey function
		println("**********sortByKey function*****************")
		mapData.sortByKey().collect.foreach(println)

		// join function
		println("**********join function*****************")
		val dat = sc.parallelize(Array(("A",1),("B",2),("C",3)))
		val dat2 = sc.parallelize(Array(("A",4),("A",6),("B",7),("C",3),("C",8)))
		val res = dat.join(dat2)
		res.collect.foreach(println)

		sc.stop()
		spark.stop()
	}
}
