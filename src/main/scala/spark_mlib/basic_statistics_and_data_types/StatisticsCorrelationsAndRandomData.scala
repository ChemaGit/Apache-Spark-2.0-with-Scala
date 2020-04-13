package spark_mlib.basic_statistics_and_data_types

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object StatisticsCorrelationsAndRandomData {
  val spark = SparkSession
    .builder()
    .appName("StatisticsCorrelationsAndRandomData")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","StatisticsCorrelationsAndRandomData") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    /**
     * 	- Column summary statistics for an instance of RDD[Vector] are
     * available through the colStats() function in Statistics
     * 	- It returns an instance of MultivariateStatisticalSummary, which
     * contains column-wise reusts for:
     * 		- min, max
     * 		- mean, variance
     * 		- numNonzeros
     * 		- normL1, normL2
     * 	- count returns the total count of elements
     */

    val observations: RDD[Vector] = sc.parallelize(Array(
      Vectors.dense(1.0,2.0),
      Vectors.dense(4.0,5.0),
      Vectors.dense(7.0,8.0)))

    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)

    println(summary.mean)
    println(summary.variance)
    println(summary.numNonzeros)
    println(summary.normL1)
    println(summary.normL2)

    /**
     * PEARSON CORRELATION BETWEEN TWO SERIES
     */
    val x: RDD[Double] = sc.parallelize(Array(2.0,9.0, -7.0))
    val y: RDD[Double] = sc.parallelize(Array(1.0,3.0, 5.0))
    val correlation: Double = Statistics.corr(x, y, "pearson")
    println(s"Pearson correlation between two series: $correlation")

    /**
     * PEARSON CORRELATION AMONG SERIES
     */
    val data: RDD[Vector] = sc.parallelize(Array(
      Vectors.dense(2.0,9.0,-7.0),
      Vectors.dense(1.0,-3.0,5.0),
      Vectors.dense(4.0,0.0,-5.0)))
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")
    println(s"Pearson correlation among series: $correlMatrix")

    /**
     * PEARSON VS SPEARMAN CORRELATION AMONG SERIES
     */
    val ranks: RDD[Vector] = sc.parallelize(Array(Vectors.dense(1.0,2.0,3.0),
      Vectors.dense(5.0,6.0,4.0),
      Vectors.dense(7.0,8.0,9.0)))
    val corrPearsonMatrix: Matrix = Statistics.corr(ranks, "pearson")
    println(s"Pearson correlation among series: $corrPearsonMatrix")
    val corrSpearmanMatrix: Matrix = Statistics.corr(ranks, "spearman")
    println(s"Spearman correlation among series: $corrSpearmanMatrix")

    /**
     * RANDOM DATA GENERATION
     * 	- RandomRDDs generate either random double RDDs or vector RDDs
     * 	- Supported distributions
     * 		- uniform, normal, lognormal, poisson, exponential, and gamma
     * 	- Useful for randomized algorithms, prototyping and performance testing
     */

    /**
     * Simple example of generationg random data
     */
    val million = poissonRDD(sc, mean=1.0, size=1000000L,numPartitions=10)
    println(s"mean: ${million.mean}")
    println(s"variance: ${million.variance}")

    /**
     * Simple Vector Example (Multivariate Normal)
     */
    val dataR = normalVectorRDD(sc, numRows=10000L,numCols=3,numPartitions=10)

    val stats: MultivariateStatisticalSummary = Statistics.colStats(dataR)
    println(s"mean: ${stats.mean}")
    println(s"variance: ${stats.variance}")

    sc.stop()
    spark.stop()
  }
}
