package spark_sql

import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf


object SparkSQLUDF {

	val upper: String => String = str => str.toUpperCase

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Spark SQL UDF").master("local").getOrCreate()
		val sc = spark.sparkContext
		import spark.implicits._
		sc.setLogLevel("ERROR")
		
		val dataset = Seq((0, "hello"),(1, "world")).toDF("id","text")
		val upperUDF = udf(upper)
		dataset.withColumn("upper", upperUDF($"text")).show()

		spark.udf.register("myUpper",(input: String) => input.toUpperCase)
		spark.catalog.listFunctions.filter('name like "%upper%").show(false)

		spark.catalog.listFunctions.show(100,false)

		sc.stop
		spark.stop
	}
}
