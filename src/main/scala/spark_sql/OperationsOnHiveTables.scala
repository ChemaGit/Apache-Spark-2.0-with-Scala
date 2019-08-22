package spark_sql

import org.apache.spark.sql._

object OperationsOnHiveTables {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Operations On Hive Tables").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		import spark.implicits._

		sc.stop
		spark.stop
	} 
}
