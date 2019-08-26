package spark_sql

import org.apache.spark.sql._

object OperationsOnHiveTables {
	def main(args: Array[String]): Unit = {
		val warehouseLocation = "/user/hive/warehouse"
		case class Record(key: Int, value: String)

		val spark = SparkSession.builder().appName("Operations On Hive Tables").master("local")
			.config("spark.sql.warehouse.dir",warehouseLocation)
			.enableHiveSupport()
			.getOrCreate()

		spark.conf.getAll.foreach(println)

		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		import spark.implicits._

		spark.sql("show databases").show()
		spark.sql("use default")
		spark.sql("show tables").show()
		//spark.sql("select * from t_iot_devices limit 10").show()
		spark.sql("CREATE TABLE IF NOT EXISTS src1(key int, value string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
		spark.sql("show tables").show()
		spark.sql("LOAD DATA LOCAL INPATH '/home/cloudera/files/kv1.txt' INTO TABLE src1")
		spark.sql("SELECT * FROM src1").show()

		spark.sql("SELECT COUNT(*) FROM src1").show()
		
		val sqlDF = spark.sql("SELECT key,value FROM src1 WHERE KEY < 100 ORDER BY key")
		val stringDS = sqlDF.map{case Row(key: Int, value: String) => s"Key: $key, Value: $value"}
		stringDS.show()

		val recordsDF = spark.createDataFrame( (1 to 100).map(i => (i, s"val_$i"))).toDF("key", "value")

		recordsDF.createOrReplaceTempView("records")
		spark.sql("SELECT * FROM records r JOIN src1 s ON r.key = s.key").show()

		sc.stop
		spark.stop
	} 
}
