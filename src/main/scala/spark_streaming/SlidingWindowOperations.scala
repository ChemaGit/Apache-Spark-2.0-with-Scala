package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

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
  * spark-submit --master 'local[2]' --class stubs.StreamingLogs target/slidingWindowOperations-1.0.jar localhost 1234
  */

object SlidingWindowOperations {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Sliding Window Operations")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val checkPointDir = "hdfs://quickstart.cloudera/user/cloudera/checkpoint"

    val ssc = new StreamingContext(sc, Seconds(2))
    val logs = ssc.socketTextStream("quickstart.cloudera", 4444)

    ssc.checkpoint(checkPointDir)

    val reqcountsByWindow = logs
      .map(line => line.split(" "))
      .map(arr => (arr(0), 1))
      .reduceByKeyAndWindow( (v: Int, c: Int) => v + c, Minutes(5), Seconds(30))

    // Sort and print the top users for every RDD (every 30 seconds)
    val topreqsByWindow = reqcountsByWindow
        .transform(rdd => rdd.sortBy(t => t._2, false))

    topreqsByWindow.foreachRDD(rdd => rdd.foreach(println))


    ssc.start()
    ssc.awaitTermination()
  }

}
