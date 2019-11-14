package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * start_logs
  * tail_logs | nc -lk 4444
  * stop_logs
  *
  *
  * Use spark-submit to run your app locally and be sure to specify two threads: at least two threads or nodes are required to run a streaming app, while the VM cluster has only one.
  * The StreamingLogs app takes two parameters: the hostname and the port number to connect the DStream to.
  *
  *
  * spark-submit --master 'local[2]' --class stubs.StreamingLogs target/processingMultibatches-1.0.jar localhost 1234
  */

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
