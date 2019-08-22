package spark_sql

import org.apache.spark.sql._

object LoadingDataProgramatically {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Loading Data Programatically").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		import spark.implicits._

		val employeeDF = spark.read.json("hdfs://quickstart.cloudera/user/cloudera/files/employee.json")
		employeeDF.write.parquet("hdfs://quickstart.cloudera/user/cloudera/employee_parquet")

		val parquetFileDF = spark.read.parquet("hdfs://quickstart.cloudera/user/cloudera/employee_parquet")
		parquetFileDF.createOrReplaceTempView("parquetFile")
		val namesDF = spark.sql("SELECT first_name, last_name FROM parquetFile")
		namesDF.map(arr => "Full Name: %s %s ".format(arr(0),arr(1))).show()

		val path = "hdfs://quickstart.cloudera/user/cloudera/files/employee.json"
		val empDF = spark.read.json(path)
		empDF.printSchema()

		empDF.createOrReplaceTempView("emp")
		val empSalary = spark.sql("SELECT first_name, last_name, LENGTH(CONCAT(first_name,last_name)) * 10000 AS salary FROM emp")
		empSalary.show()
		empSalary.toJSON.rdd.repartition(1).saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/emp_json")


		sc.stop
		spark.stop
	}
}
