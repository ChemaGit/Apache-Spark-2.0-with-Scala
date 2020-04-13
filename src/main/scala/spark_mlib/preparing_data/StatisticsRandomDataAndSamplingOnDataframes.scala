package spark_mlib.preparing_data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * # Summary Statistics for DataFrames
 * 	- Column summary statiscis for DataFrames are available through
 * DataFrame's describe() method
 * 	- It returns another DataFrame, which contains column-wise results for:
 * 		- min, max
 * 		- mean, stddev
 * 		- Count
 * 	- Column summary statistics can also be computed through
 * DataFrame's groupBy() and agg() methods, but stddev is not supported
 * 	- It also returns another DataFrame with the results
 */

object StatisticsRandomDataAndSamplingOnDataframes {

  val spark = SparkSession
    .builder()
    .appName("StatisticsRandomDataAndSamplingOnDataframes")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","StatisticsRandomDataAndSamplingOnDataframes") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  case class Record(desc: String, value1: Int, value2: Double)

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      sc.setLogLevel("ERROR")

      import org.apache.spark.sql.functions._
      import spark.implicits._

      /**
       * An example of summary Statistics
       */
      val recDF = sc.parallelize(Array(Record("first", 1, 3.7),Record("second",-2, 2.1),Record("third", 6,0.7))).toDF
      val recStats = recDF.describe()
      recDF.printSchema()
      recStats.show()
      println()

      /**
       * Fetching Results from a DataFrame
       */
      val exp =recStats.filter("summary = 'stddev'").first()
      println(s"summary stddev: $exp")
      val exp1 = recStats.filter("summary = 'stddev'").first().toSeq.toArray
      exp1.foreach(x => println(s"summary1 stddev: $x"))
      val exp2 = recStats.filter("summary = 'stddev'").first().toSeq.toArray.drop(2).map(_.toString.toDouble)
      exp2.foreach(x => println(s"summary2 stddev: $x"))
      val exp3 = recStats.select(col("summary"), col("value1")).map(s => "%s ==>  %s".format(s(0).toString,s(1).toString)).collect()
      exp3.foreach(x => println(s"value1: $x"))
      println()


      /**
       * # Another Example
       */
      val max = recDF.groupBy().agg(Map("value1" -> "max", "value2" -> "max"))
      max.show(truncate = false)
      val min = recDF.groupBy().agg(Map("value1" -> "min", "value2" -> "min"))
      min.show(truncate = false)

      import org.apache.spark.sql.functions._

      val recStatsGroup = recDF.groupBy().agg(min("value1"), min("value2"))
      recStatsGroup.show(truncate = false)

      recStatsGroup.columns.foreach(println)
      recStatsGroup.first().toSeq.toArray.map(_.toString.toDouble).foreach(println)

      /**
       * # More Statistics on DataFrames
       * 	- More statistics are available through the stats method in a DataFrame
       * 	- It returns a DataFrameStatsFunctions object, which has the following methods:
       * 		- corr(): computes Pearson correlation between two columns
       * 		- cow(): computes sample covariance between two columns
       * 		- crosstab(): computes a pair-wise frequency table of the given columns
       * 		- freqItems(): finds frequent items for columns, possibly with false positives
       */

      /**
       * # A Simple Example of Statistics
       */
      val recDFStat = recDF.stat
      recDFStat.corr("value1", "value2")
      recDFStat.cov("value1", "value2")
      recDFStat.freqItems(Seq("value1"), 0.3).show()

      /**
       * # Sampling on DataFrames
       * 	- Can be performed on any DataFrame
       * 	- Returns a sampled subset of a DataFrame
       * 	- Sampling with or without replacement
       * 	- Fraction: expected fraction of rows to generate
       * 	- Can be used on bootstrapping procedures
       */
      val df = sqlContext.createDataFrame(Seq((1,10),(1,20),(2,10),(2,20),(2,30),(3,20),(3,30))).toDF("key", "value")
      val dfSampled = df.sample(withReplacement=false, fraction=0.3, seed=11L)
      dfSampled.show(truncate = false)

      /**
       * # Random Split on DataFrames
       * 	- Can be performed on any DataFrame
       * 	- Returns an array of DataFrames
       * 	- Weights for the split will be normalized if they do not add up to 1
       * 	- Useful for splitting a data set into training, test and validation sets
       */
      val dfSplit = df.randomSplit(weights = Array(0.3, 0.7), seed = 11L)
      dfSplit(0).show(truncate = false)
      dfSplit(1).show(truncate = false)

      /**
       * # Stratified Sampling on DataFrames
       * 	- Can be performed on any DataFrame
       * 	- Any column may work as key
       * 	- Without replacement
       * 	- Fraction: specified by key
       * 	- Available as sampleBy function in DataFrameStatFunctions
       */
      val dfStrat = df.stat.sampleBy(col="key", fractions = Map(1 -> 0.7,2 -> 0.7,3 -> 0.7),seed=11L)
      dfStrat.show(truncate = false)

      /**
       * # Random Data Generation
       * 	- sql functions to generate columns filled with random values
       * 	- Two supported distributions: uniform and normal
       * 	- Useful for randomized algorithms, prototyping and performance testing
       */
      import org.apache.spark.sql.functions.{rand, randn}

      val dfr = sqlContext.range(0,10)
      dfr.select("id").withColumn("uniform", rand(10L)).withColumn("normal", randn(10L)).show(truncate = false)

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped")
      spark.stop()
      println("SparkSession stopped")
    }
  }
}
