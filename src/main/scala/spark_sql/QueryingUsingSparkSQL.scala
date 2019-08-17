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

		case class Employee(name: String, age: Int)
		val caseClassDS = Seq(("Andrew",55),("Chema",50),("Lucia",22),("Pedro",32)).toDS
		caseClassDS.show()

		val primitiveDS = Seq(1,2,3,4,5).toDS
		primitiveDS.map(_ + 1).collect.foreach(println)

		val path = "hdfs://quickstart.cloudera/user/cloudera/files/employee.json"
		case class EmployeeName(fn: String, ln: String)
		val employeeDS = spark.read.json(path)
		employeeDS.show()

		import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
		import org.apache.spark.sql.Encoder
		import spark.implicits._

		val employeeDF = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/EmployeeName.csv").map(line => line.split(",")).map(arr => (arr(0),arr(1))).toDF("id","name")
		employeeDF.show()
		employeeDF.createOrReplaceTempView("employee")
		val emps = spark.sql("SELECT id,name FROM employee WHERE id IN('E01','E03','E05')")
		emps.show()
	
		emps.map(r => "Name: %s, Id: %s".format(r(1),r(0))).show()

		sc.stop
		spark.stop
	}
}
