package playing_with_rdds

import org.apache.spark.sql._

object SparkParallelProcessing {

  def printFirstLine(iter: Iterator[String]) = {
    println(iter.next())
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Parallel Processing").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/*")
    data.foreachPartition(printFirstLine)
    println()
    /**Average	Word	Length	by	Letter*/
    val dataFile = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/2014-03-01.log")
    val avglens = dataFile.flatMap(line => line.split("\\W")).map(w => (w, w.length)).groupByKey().map({case(k,values) => (k, values.sum/values.size)}).sortBy(t => t._2, false)
    avglens.foreach(println)
    avglens.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/results/avglens")
    println(avglens.toDebugString)

    sc.stop()
    spark.stop()
  }
}
