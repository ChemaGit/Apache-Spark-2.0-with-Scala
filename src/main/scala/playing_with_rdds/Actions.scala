package playing_with_rdds

import org.apache.spark.sql.SparkSession

object Actions {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("ActionsExample").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		/**********count Action***************/
		println("***********count Action*******************")
		val data = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/file_1.txt")
		val mapFile = data.flatMap(line => line.split(" ")).filter(w => w.toLowerCase == "hadoop")
		println(mapFile.count())
		
		/**********collect Action***************/
		println("************collect Action************************")
		val rdd = sc.parallelize(Array(("A",1),("B",2),("C",3)))
		val rdd1 = sc.parallelize(Array(("A",4),("A",6),("B",7),("C",3),("C",8)))
		val res = rdd.join(rdd1)
		res.collect.foreach(println)

		/**********take Action***************/
		println("************take Action************************")
		val rdd2 = sc.parallelize(Array(("k",5),("s",3),("s",4),("p",7),("p",5),("t",8),("k",6)),3)
		val group = rdd2.groupByKey().collect
		val twoRec = group.take(2)
		twoRec.foreach(println)

		/**********top Action***************/
		println("************top Action************************")
		val rdd3 = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/file_2.txt")
		val mapTop = rdd3.map(line => (line, line.length))
		val res1 = mapTop.top(3)
		res1.foreach(println)

		/**********collect countByValue***************/
		println("************collect countByValue************************")
		val rdd4 = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/file_3.txt")
		val res2 = rdd4.flatMap(line => line.split("\\W")).countByValue
		res2.foreach(println)

		/**********reduce Action***************/
		println("************reduce Action************************")
		val rdd5 = sc.parallelize(List(20,34,12,34,56,7,3,89,100))
		val res3 = rdd5.reduce((v,v1) => v * v1)
		val res4 = rdd5.reduce((v, v1) => v + v1)
		println(s"product: $res3, sum: $res4")

		/**********foreach Action***************/
		println("************foreach Action************************")
		val rdd6 = sc.parallelize(Array(("k",5),("s",3),("s",4),("p",7),("p",5),("t",8),("k",6)),3)		
                val group1 = rdd6.groupByKey().collect
                group1.foreach(println)

		sc.stop()
		spark.stop()
	}
}
