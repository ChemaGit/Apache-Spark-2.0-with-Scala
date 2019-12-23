package spark_sql

import org.apache.spark.sql._
import org.apache.spark.sql.types._


object SparkSQLExample {

	def mapper(lines: String) = {
		val fields = lines.split(",")
		Row(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
	}

	def main(args: Array[String]): Unit = {
		val warehouseLocation = "hdfs://quickstart.cloudera/user/hive/warehouse"
		val spark = SparkSession.builder()
			.appName("Spark SQL Example")
			.master("local[*]")
			.enableHiveSupport()
			.config("spark.sql.warehouse.dir",warehouseLocation)
			.getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")


		val data = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/amigos.csv")

		val personasRDD = data.map(mapper)

		// Create a table schema
		val schema = StructType( Array(
			StructField("ID", IntegerType, true),
			StructField("name", StringType, true),
			StructField("age", IntegerType, true),
			StructField("numFriends", IntegerType, true)
		))

		// Create a DataFrame with the Array of Rows and the schema
		val personasDF = spark.createDataFrame(personasRDD, schema)

		// A view of the data and the schema
		personasDF.printSchema
		personasDF.show()
		
		// Create a temporal view and do a filter select
		personasDF.createOrReplaceTempView("t_view_personas")
		val teens = spark.sql("""SELECT * FROM t_view_personas WHERE age BETWEEN 13 AND 19""")

		teens.collect.foreach(println)

		// Save the DataFrame in a Hive Table
		teens.write.saveAsTable("t_young_people")

		spark.sql("show tables").show()
		spark.sql("""SELECT * FROM t_young_people LIMIT 20""").show()

		sc.stop()
		spark.stop()		
	}
}
