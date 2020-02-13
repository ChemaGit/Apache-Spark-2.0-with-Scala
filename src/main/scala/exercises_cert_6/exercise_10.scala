package exercises_cert_6

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

/**
  * Question 3: Correct
  * PreRequiste:
  * [Prerequisite section will not be there in actual exam]
  *
  * Run below sqoop command to import orders table from mysql into hdfs to the destination /user/cloudera/problem2/avro as avro file.
  * sqoop import \
  * --connect "jdbc:mysql://localhost:3306/retail_db" \
  * --password cloudera \
  * --username root \
  * --table orders \
  * --as-avrodatafile \
  * --target-dir /user/cloudera/problem2/avro \
  * --outdir /home/cloudera/outdir \
  * --bindir /home/cloudera/bindir
  *
  * Instructions:
  *
  * Convert data-files stored at hdfs location /user/cloudera/problem2/avro into parquet file using snappy compression and save in HDFS.
  *
  * Output Requirement:
  *
  * Result should be saved in /user/cloudera/problem2/parquet-snappy
  * Output file should be saved as Parquet file in Snappy Compression.
  */

object exercise_10 {

  val spark = SparkSession
    .builder()
    .appName("exercise 10")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_10")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    val pathIn = "hdfs://quickstart.cloudera/user/cloudera/problem2/avro"
    val pathOut = "hdfs://quickstart.cloudera/user/cloudera/problem2/parquet"
    try {
      import com.databricks.spark.avro._
      val ordersDF = sqlContext
          .read
          .avro(pathIn)
          .cache()

      ordersDF.show()

      sqlContext
          .setConf("spark.sql.parquet.compression.codec","snappy")

      ordersDF
          .write
          .parquet(pathOut)

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }

  }

}
