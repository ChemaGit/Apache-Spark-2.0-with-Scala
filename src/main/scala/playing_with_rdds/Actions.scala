package playing_with_rdds

import org.apache.spark.sql.SparkSession

object Actions {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("ActionsExample").master("local").getOrCreate()
		val sc = spark.SparkContext()
		sc.setLogLevel("ERROR")
	}
}
