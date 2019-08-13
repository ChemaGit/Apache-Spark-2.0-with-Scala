package advanced_spark_programming

import org.apache.spark.sql.SparkSession

object NumericRDDOperations {
/**
* List of Numeric Methods
* count() -> Number of elements in the RDD
* mean() -> Average of the elements in the RDD
* max() -> Maximum value among all elements in the RDD
* min() -> Minimum value among all elements in the RDD
* Variance() -> Variance of the elements
* sum() -> Total value of the elements in the RDD
* stdev() -> Standard deviation
*/
	def main(args: Array[String]):Unit = {
		val spark = SparkSession.builder.appName("Numeric RDD Operations").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val data = sc.parallelize(List(1,2,3,4,5,6,7,8,9))
		println("count: " + data.count)
		println("mean: " + data.mean)
		println("max: " + data.max)
		println("min: " + data.min)
		println("variance: " + data.variance)
		println("sum: " + data.sum)
		println("stdev: " + data.stdev)

		sc.stop()
		spark.stop()
	}
}
