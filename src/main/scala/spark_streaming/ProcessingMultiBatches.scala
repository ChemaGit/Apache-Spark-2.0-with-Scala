package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * STATE OPERATIONS
  */
object ProcessingMultiBatches {

  def updateCount = (newCounts: Seq[Int], state: Option[Int]) => {
    val newCount = newCounts.foldLeft(0)( (v, c) => v + c)
    val previousCount = state.getOrElse(0)
    Some(newCount + previousCount)
  }

  def updateCount2(newCounts: Seq[Int], state: Option[Int]):Option[Int] = {
    val newCount = newCounts.foldLeft(0)( (v, c) => v + c)
    val previousCount = state.getOrElse(0)
    Some(newCount + previousCount)
  }

  def main(args: Array[String]): Unit = {
    val checkpointDir = "hdfs://quickstart.cloudera/user/cloudera/checkpoint"

    val spark = SparkSession
      .builder()
      .appName("Processing Multi Batches")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc,Seconds(5))
    sc.setLogLevel("ERROR")

    val logsStream = ssc.socketTextStream("quickstart.cloudera",4444)
    // logsStream.foreachRDD(rdd => rdd.foreach(arr => println(arr)))
    val logLines = logsStream.map(line => line.split(" "))
      .map(arr => (arr(0), 1))
      .reduceByKey( (v,c) => v + c)
    //logLines.foreachRDD(rdd => rdd.foreach(arr => println(arr)))

    ssc.checkpoint(checkpointDir)

    val totalUserreqs = logLines.updateStateByKey(updateCount)
    totalUserreqs.foreachRDD(rdd => rdd.foreach(arr => println(arr)))


    ssc.start()
    ssc.awaitTermination()
  }

}
