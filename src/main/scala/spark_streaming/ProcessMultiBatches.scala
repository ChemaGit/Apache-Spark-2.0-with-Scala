package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ProcessMultiBatches {

  def transform(inf: ApacheAccessLog): (String, (Long, Long)) = {
    val ip0 = inf.getIpAddress() //.split('.')(0)
    val count = inf.getContentSize()
    (ip0,(1L,count))
  }

  def addTuples(t1: (Long, Long), t2: (Long, Long)): (Long, Long) = {
    (t1._1 + t2._1, t1._2 + t2._2)
  }

  def updateState(news: Seq[(Long, Long)], state: Option[(Long, Long)]): Option[(Long, Long)] = {
    val sum = news.foldLeft((0L,0L))((x, y) => addTuples(x, y))
    val previous = state.getOrElse(0L, 0L)
    Some( addTuples(sum, previous))
  }

  def main(args: Array[String]): Unit = {
    val checkpointDir = "hdfs://quickstart.cloudera/user/cloudera/checkpoint"

    val spark = SparkSession
      .builder()
      .appName("Process Multi Batches")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc,Seconds(5))

    val weblogs = ssc.socketTextStream("quickstart.cloudera",4444)

    val times = weblogs.map(line => ApacheAccessLog.parseFromLogLine(line))
        .map(inf => transform(inf))
        .reduceByKey( (x, y) => addTuples(x, y))

    //times.print

    ssc.checkpoint(checkpointDir)

    val totalSorted = times
        .updateStateByKey( (s: Seq[(Long, Long)], state: Option[(Long, Long)]) => updateState(s, state))
        .transform(rdd => rdd.sortBy(e => e._2, false))

    totalSorted.print

    totalSorted.saveAsTextFiles("hdfs://quickstart.cloudera/user/cloudera/out/media")

    ssc.start()
    ssc.awaitTermination()
  }

}
