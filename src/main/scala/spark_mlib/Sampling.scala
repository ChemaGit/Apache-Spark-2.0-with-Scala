package spark_mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.rdd.RDD

/**
 * #SAMPLING
 * 	- Can be performed on any RDD
 * 	- Returns a sampled subset of an RDD
 * 	- Sampling with or without replacement
 * 	- Fraction:
 * 		- without replacement - expected size of the sample as fraction of RDD's size
 * 		- with replacement - expected number of times each element is chosen
 * 	- Can be used on bootstrapping procedures
 */
object Sampling {

  val spark = SparkSession
    .builder()
    .appName("Sampling")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","Sampling") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    try {
      Logger.getRootLogger.setLevel(Level.ERROR)

      // A Simple Sampling
      val elements: RDD[Vector] = sc.parallelize(Array(
        Vectors.dense(4.0,7.0,13.0),
        Vectors.dense(-2.0,8.0,4.0),
        Vectors.dense(3.0,-11.0,19.0)))

      elements.sample(withReplacement=false, fraction=0.5,seed=10L).collect().foreach(println)
      println
      elements.sample(withReplacement=false, fraction=0.5,seed=7L).collect().foreach(println)
      println
      elements.sample(withReplacement=false, fraction=0.5,seed=64L).collect().foreach(println)
      println

      /**
       * # Random Split
       * 	- Can be performed on any RDD
       * 	- Returns an array of RDDs
       * 	- Weights for the split will be normalized if they do not add up to 1
       * 	- Useful for splitting a data set into training, test and validation sets
       */
      // A Simple Random Split
      val data = sc.parallelize(1 to 1000000)
      val splits = data.randomSplit(Array(0.6,0.2,0.2), seed = 13L)
      val training = splits(0)
      val test = splits(1)
      val validation = splits(2)

      splits.map(_.count()).foreach(println)
      println
      training.take(10).foreach(println)
      println
      test.take(10).foreach(println)
      println
      validation.take(10).foreach(println)
      println

      /**
       * # Stratified Sampling
       * 	- Can be performed on RDDs of key-value pairs
       * 	- Think of keys as labels and values as an specific attribute
       *
       * 	- Two supported methods defined in PairRDDFunctions:
       * 		- sampleByKey requires only one pass over the data and provides
       * an expected sample size
       * 		- sampleByKeyExact provides the exact sampling size with 99.99%
       * confidence but requires significantly more resources
       */
      // An Approximate Stratified Sampling
      val rows: RDD[IndexedRow] = sc.parallelize(Array(
        IndexedRow(0,Vectors.dense(1.0,2.0)),
        IndexedRow(1,Vectors.dense(4.0,5.0)),
        IndexedRow(1,Vectors.dense(7.0,8.0))))

      val fractions: Map[Long, Double] = Map(0L -> 1.0, 1L -> 0.5)

      val approxSample = rows.map {
        case IndexedRow(index, vec) => (index, vec)
      }.sampleByKey(withReplacement = false, fractions, 9L)
      approxSample.collect.foreach(println)

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
