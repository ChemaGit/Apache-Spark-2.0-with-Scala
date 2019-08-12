package advanced_spark_programming

import org.apache.spark.sql.SparkSession

object LoadingAndSavingData {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Loading And Saving Data").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val user = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/user.csv")
		val format = user.map(lines => lines.split(",")).map(arr => "%s => %s => %s".format(arr(0),arr(1),arr(2)))
		format.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/user_format")

		sc.stop()
		spark.stop()
	}
}
