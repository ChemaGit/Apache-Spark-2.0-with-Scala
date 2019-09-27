package spark_sql

import org.apache.spark.sql._

object SparkSQLforETL {
	def main(args: Array[String]): Unit = {
		val warehouseLocation = "hdfs://quickstart.cloudera/user/hive/warehouse"
		val spark = SparkSession.builder()
				.appName("Use Apache Spark SQL for ETL")
				.enableHiveSupport()
				.config("spark.sql.warehouse.dir",warehouseLocation)
				.getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		sc.stop()
		spark.stop()
	}
}
