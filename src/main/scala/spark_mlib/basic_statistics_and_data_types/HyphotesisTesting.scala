package spark_mlib.basic_statistics_and_data_types

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.random.RandomRDDs.normalRDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.{KernelDensity, Statistics}
import org.apache.spark.mllib.stat.test.ChiSqTestResult
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * ### HYPOTHESIS TESTING
 * 	- Used to determine whether a result is statistically significant,
 * that is, whether it occurred by chance or not
 * 	- Supported tests:
 * 		- Pearson's Chi-Squared test for goodness of fit
 * 		- Pearson's Chi-Squared test for independence
 * 		- Kolmogorov-Smirnov test for equality of distribution
 * 	- Inputs of type RDD[LabeledPoint] are also supported, enabling feature selection
 */

object HyphotesisTesting {

  val spark = SparkSession
    .builder()
    .appName("Sampling")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","Sampling") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try  {
      /**
       * # Pearson's Chi-Squared test for goodness of fit
       * 	- Determines whether an observed frequency distribution differs from a given distribution or not
       * 	- Requires an input of type Vector containing the frequencies of the events
       * 	- It runs against a uniform distribution, if a second vector to test against is not supplied
       * 	- Available as chiSqTest() function in Statistics
       *
       * # Testing for goodness of Fit
       */
      val vec: Vector = Vectors.dense(0.3,0.2,0.15,0.1,0.1,0.1,0.05)
      val goodnessOfFitTestResult = Statistics.chiSqTest(vec)
      println(s"Pearson's Chi-Squared test for goodness of fit: $goodnessOfFitTestResult")
      println()

      /**
       * # Pearson's Chi-Squared test for independence
       * 	- Determines whether unpaired observations on two variables are independent of each other
       * 	- Requires an input of type Matrix, representing a
       * contingency table, or an RDD[LabeledPoint]
       * 	- Available as chiSqTest() function in Statistics
       * 	- May be used for feature selection
       *
       * # Testing for independence
       */
      val mat: Matrix = Matrices.dense(3,2,Array(13.0,47.0,40.0,80.0,11.0,9.0))
      val independenceTestResult = Statistics.chiSqTest(mat)
      println(s"Pearson's Chi-Squared test for independence $independenceTestResult")
      println()

      /**
       * # Another Simple Test for Independence
       */
      val obs: RDD[LabeledPoint] = sc.parallelize(Array(
        LabeledPoint(0,Vectors.dense(1.0,2.0)),
        LabeledPoint(0,Vectors.dense(0.5,1.5)),
        LabeledPoint(0,Vectors.dense(1.0,8.0))))
      println("Another Simple Test for Independence")
      val featureTestResults: Array[ChiSqTestResult] = Statistics.chiSqTest(obs)
      featureTestResults.foreach(println)
      println()

      /**
       * # Kolmogorov-Smirnov Test
       * 	- Determines whether or not two probability distributions are equal
       * 	- One sample, two sided test
       * 	- Supported distributions to test against:
       * 		- normal distribution(distName='norm')
       * 		- customized cumulative density function(CDF)
       * 	- Available as kolmogorovSmirnovTest() function in Statistics
       *
       * # Test for Equality of Distribution
       */
      val data: RDD[Double] = normalRDD(sc, size=100,numPartitions=1, seed=13L)
      val testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
      println(s"Kolmogorov-Smirnov Test: $testResult")
      println()
      // repeat the test and the result will be different
      val testResult2 = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
      println(s"Kolmogorov-Smirnov Test: $testResult2")
      println()

      /**
       * # Kernel Density Stimation
       * 	- Computes an estimate of the probability density function of a
       * random variable, evaluated at a given set of points
       * 	- Does not require assumptions about the particular
       * distribution that the observed samples are drawn from
       * 	- Requires an RDD of samples
       * 	- Available as estimate() function in KernelDensity
       * 	- In Spark, only Gaussian kernel is supported
       */
      // # Kernel Density Estimation I
      val data1: RDD[Double] = normalRDD(sc, size=1000, numPartitions=1, seed=17L)
      val kd = new KernelDensity().setSample(data1).setBandwidth(0.1)
      val densities = kd.estimate(Array(-1.5, -1, -0.5, 1, 1.5))
      println(s"Kernel Density Estimation I:")
      densities.foreach(println)
      println()

      // # Kernel Density Estimation II
      val dataII: RDD[Double] = normalRDD(sc, size=1000, numPartitions=1, seed=17L)
      val kdII = new KernelDensity().setSample(dataII).setBandwidth(0.1)
      val densitiesII = kdII.estimate(Array(-0.25, 0.25, 0.5, 0.75, 1.25))
      println(s"Kernel Density Estimation II:")
      densitiesII.foreach(println)
      println()

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
