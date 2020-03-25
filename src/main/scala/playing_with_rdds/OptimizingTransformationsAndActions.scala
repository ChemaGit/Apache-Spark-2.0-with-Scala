package playing_with_rdds

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
Having completed this exercise, you should have much better knowledge of why groupByKey should be
 avoided if possible and be replaced with reduceByKey or aggregateByKey. However, you should
 understand that not all groupByKey use cases can be replaced. It depends on the dataset and the
 operation you need to perform on it. When possible, avoid groupByKey. Broadcast variables allows for a
 much more efficient join by eliminating the need for a lot of shuffles.
  */

object OptimizingTransformationsAndActions {

  val spark = SparkSession
    .builder()
    .appName("OptimizingTransformationsAndActions")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "OptimizingTransformationsAndActions")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val input = "hdfs://quickstart.cloudera/public/cognitive_class/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      // Calculate the average duration of each start terminal using groupByKey. First you need to convert
      // the trips RDD into a Pair RDD. Use the keyBy method.
      val input1 = sc.textFile(s"${input}trips/*")
      val header1 = input1.first // to skip the header row

      val trips = input1
        .filter(_ != header1)
        .map(_.split(","))
        .map(utils.Trip.parse(_))
        .cache()

      val input2 = sc.textFile(s"${input}stations/*")
      val header2 = input2.first // to skip the header row

      val stations = input2
        .filter(_ != header2)
        .map(_.split(","))
        .map(utils.Station.parse(_))
        .cache()

      val byStartTerminal = trips
        .keyBy(_.startStation)
        .cache()
      val durationsByStart = byStartTerminal
        .mapValues(_.duration)
        .cache()

      val grouped = durationsByStart
        .groupByKey()
        .mapValues(list => list.sum/list.size)
      grouped.take(10).foreach(println)

      println()
      println()

      // As you recall from the lesson, groupByKey should be avoided when possible, because all the keyvalue
      // pairs are shuffled around. Instead, use aggregateByKey, so that the keys are combined first
      // on the same key before it is shuffled. Let's see how this is done:

      val results = durationsByStart
        .aggregateByKey((0, 0))((acc, value) => (acc._1 + value, acc._2 + 1),
          (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

      val finalAvg = results.mapValues(i => i._1 / i._2)
      finalAvg
        .take(10)
        .foreach(println)

      durationsByStart.unpersist()

      println()
      println()

      // Find the first trip starting at each terminal using groupByKey. Remember, Spark is best when
      // everything is done in-memory. However, when you use groupByKey operations on a large dataset,
      // you will likely get an OOM error because the entire dataset is stored in memory in order to be
      // grouped. This is very inefficient. We show that this works for our dataset, but it may not work if
      // the dataset is very large.

      val firstGrouped = byStartTerminal
        .groupByKey()
        .mapValues(list => list.toList.sortBy(_.startDate.getTime).head)

      println(firstGrouped.toDebugString)
      firstGrouped.take(10).foreach(println)

      println()
      println()

      // The better approach would be to use reducebyKey, which will look within its own worker node for
      // the first trip and only send out at most one to be combined with the other worker nodes (instead of
      // sending everything out to be combined):

      val firstTrips = byStartTerminal.reduceByKey((a, b) => {
        a.startDate.before(b.startDate) match {
          case true => a
          case false => b
        }
      })

      println(firstTrips.toDebugString)
      firstTrips.take(10)

      // Broadcast joins allow you to map the key directly to perform a join, rather than shuffling it for the
      // join. Remember that the broadcast variables are read-only. To create the broadcast variable:

      val bcStations = sc.broadcast(stations.keyBy(_.id).collectAsMap)

      // Join the trips and stations using a broadcast join:

      val joined = trips.map(trip =>{
        (trip, bcStations.value.getOrElse(trip.startTerminal, Nil),
          bcStations.value.getOrElse(trip.endTerminal, Nil))
      })

      println(joined.toDebugString)

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
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
