package spark_sql

import org.apache.spark.sql._
import org.apache.spark.sql.hive._

object DataFramesAndSparkSQL {
	def main(args: Array[String]): Unit = {
		val warehouseLocation = "/user/hive/warehouse"
		val spark = SparkSession.builder().appName("DataFrames And Spark SQL").master("local[*]").enableHiveSupport().config("spark.sql.warehouse.dir",warehouseLocation).getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		val sqlContext = new HiveContext(sc)
		import sqlContext.implicits._
		/*
		-DataFrames can be created
			-From an existing structured data source
				-Such as a Hive table, Parquetfile, or JSON file
			-From an existing RDD
			-By performing an operation or query on another DataFrame
			-By programmatically defining a schema
		-sqlContext.read
			-json(filename)
			-parquet(filename)
			-orc(filename)
			-table(hive-tablename)
			-jdbc(url, table, options)
		*/
		// Creating a DataFrame from a JSON File
		val peopleDF = sqlContext.read.json("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/people.json")
		peopleDF.show()

		// Creating a DataFrame from a Hive/Impala Table
		sqlContext.sql("show tables").show()
		val customerDF = sqlContext.read.table("src1")
		customerDF.show(10)

		// Loading from a Data Source Manually
		// import com.databricks.spark.avro
		val avro_data = sqlContext.read.format("com.databricks.spark.avro").load("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/accounts_avro/*")
		avro_data.show(10)

		val jdbc_data = sqlContext.read.format("jdbc")
				.option("driver","com.mysql.jdbc.Driver")
				.option("url","jdbc:mysql://quickstart.cloudera/retail_db")
				.option("dbtable", "departments_new")
				.option("user","root")
				.option("password","cloudera")
				.load()
		jdbc_data.show()

		// Displaying column data types using dtypes
		avro_data.dtypes.foreach(println)

		/*
		- Queries-create a new DataFrame
			- DataFrames are immutable
			- Queries are analoguous to RDD transformations
		- Actions-return data to the driver
			- Actions trigger "lazy" execution of queries
		- Some Dataframes actions
			- collect, take(n), count, show(n)
		*/
		println(avro_data.count())
		jdbc_data.show(50)

		/*
		- Dataframe query methods return new DataFrames
			- Queries can be chained like transformations
		- Some query methods
			- distinct, join, limit, select, where(alias for filter)
		*/
		jdbc_data.limit(5).show()
		jdbc_data.select("department_id","department_name").show(45)
		jdbc_data.where("department_id between 5 and 25").show()

		// Columns can be referenced in multiple ways
		val dept = jdbc_data.select(jdbc_data("department_name"))
		dept.show()
		val dept_name = jdbc_data.select($"department_name")
		dept_name.show()
		
		// Column references can also be column expressions
		customerDF.select(customerDF("key")+ 10 ,customerDF("value")).show(50)
		customerDF.sort(customerDF("key").desc).show(50)

		// Joining DataFrames
		// A basic inner join when join column is in both DataFrames
		val pcodes = sqlContext.read.json("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/pcodes.json")

		val joined = peopleDF.join(pcodes, "pcode")
		joined.show()

		// Specify type of join as inner(default), outer, left_outer, right_outer, or leftsemi
		val left_join = peopleDF.join(pcodes,Array("pcode"), "left_outer")
		left_join.show()

		// Use a column expression when column names are different		
		val joined2 = jdbc_data.join(customerDF,$"department_id" === $"key")
		joined2.show()

		// When using HiveContext, you can query Hive/Impala tables using HiveQL. Returns a DataFrame
		val query = sqlContext.sql("""SELECT * FROM src1 WHERE value LIKE "A%" """)
		query.show()

		// You can also perform some SQL queries whith a DataFrame
		// First, register the DataFrame as a "table or view" with the SQLcontext
		customerDF.createOrReplaceTempView("customer") //customerDF.registerTempTable("customer")
		sqlContext.sql("""SELECT * FROM customer WHERE value LIKE "D%" """).show()

		// You can query directly from Parquet or JSON files without needing to create a DataFrame or register a temporary table
		sqlContext.sql("""SELECT * FROM json.`hdfs://quickstart.cloudera/user/cloudera/loudacre/data/people.json` WHERE name LIKE "A%" """).show()

		// Dataframes provide many other data manipulation and query functions such as: 
		// groupBy, orderBy, add, join, unionAll, intersect, avg, sampleBy, corr, cov, rollup, cube

		/*
		- Saving DataFrames
			- Data in DataFrames can be saved to a data source
			- Use DataFrame.write to create a DataFrameWriter
			- DataFrameWriter provides convenience functions to externally save data represented by a DataFrame
				- jdbc inserts into a new or existing table in a database
				- json saves as a JSON file
				- parquet saves as a Parquet file
				- orc saves as an ORC file
				- text saves as a text file (string data in a single column only)
				- saveAsTable saves as a Hive/Impala table (HiveContext only)
		- DataFrameWriter option methods
			- format specifies a data source type
			- mode determines the behavior if file or table already exists: overwrite, append, ignore or error (default is error)
			- partitionBy stores data in partitioned directories in the form column=value(as with Hive/Impala partitioning)
			- options specifies properties for the target data source
			- save is the generic base function to write the data
		*/

		// peopleDF.write.saveAsTable("t_people")
		// jdbc_data.write.saveAsTable("t_departments")
		customerDF.write.format("parquet").mode("append").partitionBy("key").saveAsTable("t_customer")

		// DataFrames are built on RDDs
		// Row RDDs have all the standard Spark actions and transformations: collect, take, count, map, flatMap, filter
		// Row RDDs can be transformed into pair RDDs to use map-reduce methods
		// DataFrames also provide convenience methods(such as map, flatMap, and foreach) for converting to RDDs
		val custRDD = customerDF.rdd

		// Working with Row objects, Scala
		// Use Array-like syntax to return values with type Any: row(n) returns element int nth column
		// Use methods to get correctly typed values: row.getAs[Long]("key")
		// Use type-specific get methods to return typed values: row.getString(n) returns nth column as a String, row.getInt(n) and so on
		val rdd = custRDD.map(row => (row(row.fieldIndex("key")), row(row.fieldIndex("value"))))
		val custByKey = rdd.groupByKey()
		custByKey.collect.foreach(println)

		// You can also create a DF from an RDD using createDataFrame
		import org.apache.spark.sql.types._
		import org.apache.spark.sql.Row

		val schema = StructType(Array(StructField("age",IntegerType,true),StructField("name",StringType,true),StructField("pcode",StringType,true)))
		val rowrdd = sc.parallelize(Array(Row(40,"Abram","01601"),Row(16,"Lucia","87501")))
		val mydf = sqlContext.createDataFrame(rowrdd, schema)
		mydf.show()

		sqlContext.sql("""show tables""").show()

		/*
		- Spark 2.0 is the next major release of Spark
		- Several significant changes related to Spark SQL, including
			- SparkSession replaces SQLContext and HiveContext
			- Support for ANSI-SQL as well as HiveQL
			- Support for subqueries
			- Support for Datasets

		- Spark SQL is not a replacement for a database, or a specialized SQL engine like Impala
			- Spark SQL is most useful for ETL or incorporating structured data into a Spark application
		*/
		
		sc.stop()
		spark.stop()
	}
}
