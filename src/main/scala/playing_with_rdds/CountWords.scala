package playing_with_rdds

import org.apache.spark.sql.SparkSession

object CountWords {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Count Words")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // Create an RDD
    val rdd = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/Content.txt")

    val count = rdd.flatMap(line => line.split("\\W"))
      .map(r => (r, 1))
      .reduceByKey( (v, c) => v + c)
      .sortBy(t => t._2, false)

    count.collect.foreach(x => println(x))

    sc.stop()
    spark.stop()
  }
}
