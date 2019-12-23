package spark_sql

import org.apache.spark.sql._

/*
1. In a terminal window, use Sqoop to import the webpage table from MySQL. 
   Use Parquet file format

$ sqoop import \
--connect jdbc:mysql://quickstart.cloudera/loudacre \
--username root \
--password cloudera \
--table webpage \
--delete-target-dir \
--target-dir /user/cloudera/loudacre/data/webpage \
--as-parquetfile \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 8

$ parquet-tools cat hdfs://quickstart.cloudera/user/cloudera/loudacre/data/webpage/6aef3171-24b3-4d60-9e21-f93051bcd0bc.parquet
*/

object SparkSQLforETL {
	def main(args: Array[String]): Unit = {
		val warehouseLocation = "hdfs://quickstart.cloudera/user/hive/warehouse"
		val spark = SparkSession.builder()
				.appName("Use Apache Spark SQL for ETL")
				.master("local[*]")
				.enableHiveSupport()
				.config("spark.sql.warehouse.dir",warehouseLocation)
				.getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		import spark.implicits._

		// Create a DataFrame from the webpage table
		val webpageDF = spark.read.parquet("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/webpage/")

		// Show the schema for the webpage table dataframe
		webpageDF.printSchema()

		// View the first few records
		webpageDF.show(5)

		// Create a new DF by selecting thow columns of the first DF
		val assocFilesDF = webpageDF.select($"web_page_num",$"associated_files")

		// Print the schema of the new DF
		assocFilesDF.printSchema

		// Show the first few rows of the DF
		assocFilesDF.show(5)

		// Create an RDD from the DF, and extract the column values from the Row items into a pair
		val aFilesRDD = assocFilesDF.map(row => (row.getAs[Int]("web_page_num"), row.getAs[String]("associated_files")))
		aFilesRDD.collect.foreach(println)

		// Split the list of files names in the second column
		val aFilesRDD2 = aFilesRDD.rdd.flatMapValues(filestring => filestring.split(","))

		// Convert the RDD to a RowRDD
		import org.apache.spark.sql.Row
		val aFilesRowRDD = aFilesRDD2.map(pair => Row(pair._1, pair._2))

		// Convert back to a DataFrame
		val aFileDF = spark.createDataFrame(aFilesRowRDD, assocFilesDF.schema)
		aFileDF.printSchema
		aFileDF.show(10)

		// Give the new DataFrame a more accurate column name
		val finalDF = aFileDF.withColumnRenamed("associated_files","associated_file")
		finalDF.printSchema
		finalDF.show(10)

		// Save as files, overwriting any prior data in the directory
		finalDF.write.mode("overwrite").save("hdfs://quickstart.cloudera/user/cloudera/loudacre/results/webpage_files")

		// After saving, download and review one of the saved files
		// hdfs dfs -ls /user/cloudera/loudacre/results/webpage_files
		// $ parquet-tools schema hdfs://quickstart.cloudera/user/cloudera/loudacre/results/webpage_files/part-00000-d2ef46a7-34db-48a2-9f26-9970361be913-c000.snappy.parquet
		// parquet-tools head hdfs://quickstart.cloudera/user/cloudera/loudacre/results/webpage_files/part-00000-d2ef46a7-34db-48a2-9f26-9970361be913-c000.snappy.parquet
		

		sc.stop()
		spark.stop()
	}
}
