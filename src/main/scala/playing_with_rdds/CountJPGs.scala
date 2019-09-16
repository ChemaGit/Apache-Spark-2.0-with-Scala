package playing_with_rdds

import org.apache.spark.sql._

// sbt:Count JPGs> runMain CountJPGs hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/*
// spark2-submit --class CountJPGs --master "local" target/scala-2.11/count-jpgs_2.11-0.1.jar hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/*
// spark2-submit --class CountJPGs --master "yarn-client" target/scala-2.11/count-jpgs_2.11-0.1.jar hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/*
// spark2-submit --class CountJPGs --master "yarn" target/scala-2.11/count-jpgs_2.11-0.1.jar hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/*

object CountJPGs {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: CountJPGs <logfile>")
      System.exit(1)
    }
    val mydir = args(0)
    val spark = SparkSession.builder().appName("Count JPGs").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val rdd = sc.textFile(mydir).filter(line => line.contains(".jpg")).count()

    println("El numero de peticiones .jpg son: " + rdd)

    sc.stop()
    spark.stop()
  }
}
