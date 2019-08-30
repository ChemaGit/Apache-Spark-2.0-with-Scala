package spark_mlib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql._

object MLibAlgorithm {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Mlib Algorithm").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val observations = sc.parallelize(
			Seq(
				Vectors.dense(1.0,10.0,100.0),
				Vectors.dense(2.0,20.0,200.0),
				Vectors.dense(3.0,30.0,300.0)
			)
		)
		// Compute column summary statistics
		val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
		println(s"Max: ${summary.max}")
		println(s"Min: ${summary.min}")
		println(s"Mean: ${summary.mean}") // a dense vector containing the mean value for each column
		println(s"Variance: ${summary.variance}") // column-wise variance
		println(s"Num Non zeros: ${summary.numNonzeros}") // number of nonzeros in each column

		sc.stop()
		spark.stop()
	}

}
