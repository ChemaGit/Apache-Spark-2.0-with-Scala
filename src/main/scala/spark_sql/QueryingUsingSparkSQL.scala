package spark_sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions.concat_ws

object QueryingUsingSparkSQL {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Querying using Spark SQL").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		import spark.implicits._

		val df = spark.read.json("hdfs://quickstart.cloudera/user/cloudera/files/employee.json")
		df.show()
		df.printSchema
		df.select("first_name").show()
		df.selectExpr("concat(first_name,'  ',last_name) as full_name").show()
		df.filter("length(first_name) > 5").show()
		df.groupBy("first_name").count().show(50)
		df.createOrReplaceTempView("employee")
		val sqlDF = spark.sql("SELECT * FROM employee")
		sqlDF.show(100)

		sc.stop
		spark.stop
	}
}
