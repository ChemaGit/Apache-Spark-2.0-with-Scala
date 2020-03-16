package exercises_cert_6

/**
  * Problem 4:
  * 1. Import orders table from mysql as text file to the destination /user/cloudera/problem5/text.
  * Fields should be terminated by a tab character ("\t") character and lines should be terminated by new line character ("\n").
  * 2. Import orders table from mysql  into hdfs to the destination /user/cloudera/problem5/avro. File should be stored as avro file.
  * 3. Import orders table from mysql  into hdfs  to folders /user/cloudera/problem5/parquet. File should be stored as parquet file.
  * 4. Transform/Convert data-files at /user/cloudera/problem5/avro and store the converted file at the following locations and file formats
  * -save the data to hdfs using snappy compression as parquet file at /user/cloudera/problem5/parquet-snappy-compress
  * -save the data to hdfs using gzip compression as text file at /user/cloudera/problem5/text-gzip-compress
  * -save the data to hdfs using no compression as sequence file at /user/cloudera/problem5/sequence
  * -save the data to hdfs using snappy compression as text file at /user/cloudera/problem5/text-snappy-compress
  * 5. Transform/Convert data-files at /user/cloudera/problem5/parquet-snappy-compress and store the converted file at the following locations and file formats
  * -save the data to hdfs using no compression as parquet file at /user/cloudera/problem5/parquet-no-compress
  * -save the data to hdfs using snappy compression as avro file at /user/cloudera/problem5/avro-snappy
  * 6. Transform/Convert data-files at /user/cloudera/problem5/avro-snappy and store the converted file at the following locations and file formats
  * -save the data to hdfs using no compression as json file at /user/cloudera/problem5/json-no-compress
  * -save the data to hdfs using gzip compression as json file at /user/cloudera/problem5/json-gzip
  * 7. Transform/Convert data-files at  /user/cloudera/problem5/json-gzip and store the converted file at the following locations and file formats
  * -save the data to as comma separated text using gzip compression at   /user/cloudera/problem5/csv-gzip
  * 8. Using spark access data at /user/cloudera/problem5/sequence and stored it back to hdfs using no compression as ORC file to HDFS to destination /user/cloudera/problem5/orc
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/*
1. Import orders table from mysql as text file to the destination /user/cloudera/problem5/text.
    Fields should be terminated by a tab character ("\t") character and lines should be terminated by new line character ("\n").
$ sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--as-textfile \
--fields-terminated-by '\t' \
--lines-terminated-by '\n' \
--target-dir /user/cloudera/problem5/text \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

2. Import orders table from mysql  into hdfs to the destination /user/cloudera/problem5/avro. File should be stored as avro file.
$ sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--as-avrodatafile \
--target-dir /user/cloudera/problem5/avro \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir

3. Import orders table from mysql  into hdfs  to folders /user/cloudera/problem5/parquet. File should be stored as parquet file.
$ sqoop import \
--connect jdbc:mysql://quickstart.cloudera/retail_db \
--username retail_dba \
--password cloudera \
--table orders \
--as-parquetfile \
--target-dir /user/cloudera/problem5/parquet \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir
 */

object exercise_8 {

  val spark = SparkSession
    .builder()
    .appName("exercise_8")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") // Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_8")  // To silence Metrics warning.
    .getOrCreate()

  val sc = spark.sparkContext

  val out = true // to print or not in the console

  val path = "hdfs://quickstart.cloudera/user/cloudera/problem5/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      import spark.implicits._

      // 4. Transform/Convert data-files at /user/cloudera/problem5/avro and store the converted file at the following locations and file formats
      import com.databricks.spark.avro._
      val ordersAvro = spark
          .sqlContext
          .read
          .avro(s"${path}avro")
          .cache

      //   -save the data to hdfs using snappy compression as parquet file at /user/cloudera/problem5/parquet-snappy-compress
      spark
          .sqlContext
          .setConf("spark.sql.parquet.compression.codec","snappy")

      ordersAvro
          .write
          .parquet(s"${path}parquet-snappy-compress")

      //   -save the data to hdfs using gzip compression as text file at /user/cloudera/problem5/text-gzip-compress
      ordersAvro
          .rdd
          .map(r => r.mkString(","))
          .saveAsTextFile(s"${path}text-gzip-compress", classOf[org.apache.hadoop.io.compress.GzipCodec])

      //   -save the data to hdfs using no compression as sequence file at /user/cloudera/problem5/sequence
      ordersAvro
          .rdd
          .map(r => (r(0).toString, r.mkString(",")))
          .saveAsSequenceFile(s"${path}sequence")

      //   -save the data to hdfs using snappy compression as text file at /user/cloudera/problem5/text-snappy-compress
      ordersAvro
          .rdd
          .map(r => r.mkString(","))
          .saveAsTextFile(s"${path}text-snappy-compress", classOf[org.apache.hadoop.io.compress.SnappyCodec])

      // 5. Transform/Convert data-files at /user/cloudera/problem5/parquet-snappy-compress and store the converted file at the following locations and file formats
      val parquetSnappy = spark
          .sqlContext
          .read
          .parquet(s"${path}parquet-snappy-compress")
          .cache()

      //  -save the data to hdfs using no compression as parquet file at /user/cloudera/problem5/parquet-no-compress
      spark
        .sqlContext
        .setConf("spark.sql.parquet.compression.codec","uncompressed")

      parquetSnappy
          .write
          .parquet(s"${path}parquet-no-compress")

      //  -save the data to hdfs using snappy compression as avro file at /user/cloudera/problem5/avro-snappy
      spark
        .sqlContext
        .setConf("spark.sql.avro.compression.codec","snappy")

      parquetSnappy
          .write
          .avro(s"${path}avro-snappy")

      // 6. Transform/Convert data-files at /user/cloudera/problem5/avro-snappy and store the converted file at the following locations and file formats
      val avroSnappy = spark
          .sqlContext
          .read
          .avro(s"${path}avro-snappy")
          .cache()

      //  -save the data to hdfs using no compression as json file at /user/cloudera/problem5/json-no-compress
      avroSnappy
          .toJSON
          .rdd
          .saveAsTextFile(s"${path}json-no-compress")

      //  -save the data to hdfs using gzip compression as json file at /user/cloudera/problem5/json-gzip
      avroSnappy
        .toJSON
        .rdd
        .saveAsTextFile(s"${path}json-gzip",classOf[org.apache.hadoop.io.compress.GzipCodec])

      // 7. Transform/Convert data-files at  /user/cloudera/problem5/json-gzip and store the converted file at the following locations and file formats
      val jsonGzip = spark
          .sqlContext
          .read
          .json(s"${path}json-gzip")
          .cache

      //  -save the data to as comma separated text using gzip compression at   /user/cloudera/problem5/csv-gzip
      jsonGzip
          .rdd
          .map(row => row.mkString(","))
          .saveAsTextFile(s"${path}csv-gzip",classOf[org.apache.hadoop.io.compress.GzipCodec])

      jsonGzip
          .write
          .format("com.databricks.spark.csv")
          .option("compression","gzip")
          .save(s"${path}csv-gzip")

      // 8. Using spark access data at /user/cloudera/problem5/sequence and stored it back to hdfs using no compression as ORC file to HDFS to destination /user/cloudera/problem5/orc
      val sequence = sc
          .sequenceFile(s"${path}sequence",classOf[org.apache.hadoop.io.Text],classOf[org.apache.hadoop.io.Text])
          .map(t => t._2.toString)
          .map(line => line.split(","))
          .map(r => (r(0), r(1), r(2), r(3)))
          .toDF
          .cache()

      sequence
        .write
        .orc(s"${path}orc")

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      if(out) println("SparkContext stopped")
      spark.stop()
      if(out) println("SparkSession stopped")
    }
  }
}
