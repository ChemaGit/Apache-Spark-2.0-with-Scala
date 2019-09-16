package playing_with_rdds

import org.apache.spark.sql._

// sbt:Word Count> runMain WordCount hdfs://quickstart.cloudera/user/cloudera/loudacre/data/frostroad.txt
// spark2-submit --class WordCount --master "local" target/scala-2.11/word-count_2.11-0.1.jar hdfs://quickstart.cloudera/user/cloudera/loudacre/data/frostroad.txt
// spark2-submit --class WordCount --master "yarn-client" target/scala-2.11/word-count_2.11-0.1.jar hdfs://quickstart.cloudera/user/cloudera/loudacre/data/frostroad.txt
// spark2-submit --class WordCount --master "yarn" target/scala-2.11/word-count_2.11-0.1.jar hdfs://quickstart.cloudera/user/cloudera/loudacre/data/frostroad.txt

// spark jobs history ==> http://quickstart.cloudera:18089

object WordCount {
  def main(args: Array[String]): Unit = {
    val file = if(args.length < 1) {
      System.err.println("Usage: spark-submit --class WordCount MyJarFile.jar <fileURL>")
      System.exit(1)
    } else {
      args(0)
    }

    val spark = SparkSession.builder().appName("Word Count").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val data = sc.textFile(args(0)).flatMap(line => line.split("\\W")).map(w => (w, 1)).reduceByKey( (v, v1) => v + v1).sortBy(t => t._2, false)

    data.collect.foreach(println)

    sc.stop()
    spark.stop()
  }
}