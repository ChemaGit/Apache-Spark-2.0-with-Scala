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
  * spark-submit --master 'local[2]' --class stubs.StreamingLogs target/streamlog-1.0.jar localhost 1234
  */

object StreamingLogs {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Streaming Logs").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(10))

    val logsStream = ssc.socketTextStream("quickstart.cloudera",4444)
    logsStream.foreachRDD(rdd => rdd.foreach(arr => println(arr)))
    val logLines = logsStream.map(line => line.split(" "))
        .map(arr => (arr(0), 1))
        .reduceByKey( (v,c) => v + c)
    logLines.foreachRDD(rdd => rdd.foreach(arr => println(arr)))

    //logLines.saveAsTextFiles("/user/cloudera/streamlog/count-ips")

    // after setup, start the stream
    ssc.start()
    ssc.awaitTermination()
  }

}
