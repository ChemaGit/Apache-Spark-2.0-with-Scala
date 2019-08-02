package working_with_key_value_pairs

import org.apache.spark.sql.SparkSession

object WordCount {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Word Count").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		
		val filter = List("", " ")
		val prefix = "hdfs://quickstart.cloudera/user/cloudera/files/"
		val data = sc.textFile(prefix + "file_1.txt," + prefix + "file_2.txt," + prefix + "file_3.txt," + prefix + "file_4.txt")
		val words = data.flatMap(line => line.split("\\W")).filter(w => !filter.contains(w)).map(w => (w, 1))
		val wordCount = words.reduceByKey( (v,c) => v + c).sortBy(t => t._2, false)

		wordCount.collect.foreach(println)

		sc.stop()
		spark.stop()
	}
}
