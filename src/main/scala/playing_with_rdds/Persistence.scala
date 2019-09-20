package playing_with_rdds

import org.apache.spark.sql._

object Persistence {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Persistence").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		sc.stop()
		spark.stop()
	}
}
