package spark_sql

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object SpecifyingSchema {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("specifying schema").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		import spark.implicits._

		val employee = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/EmployeeName.csv")
		val schemaString = "id name"
		val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType,nullable = true))
		val schema = StructType(fields)
		val rowRDD = employee.map(line => line.split(",")).map(arr => Row(arr(0),arr(1)))
		val employeeDF = spark.createDataFrame(rowRDD, schema)
		employeeDF.show()
		employeeDF.createOrReplaceTempView("employee")
		val results = spark.sql("SELECT name FROM employee")
		results.map(arr => "Name: " + arr(0)).show()

		sc.stop
		spark.stop
	}
}
