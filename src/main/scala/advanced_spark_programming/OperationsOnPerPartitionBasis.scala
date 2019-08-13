package advanced_spark_programming

import org.apache.spark.sql.SparkSession

object OperationsOnPerPartitionBasis {
/**
* mapPartitions() -> return an new RDD by applying a function to each partition of this RDD
* mapPartitionsWithIndex() -> return a new RDD by applying a function to each partition of this RDD,
* while tracking the index of the original partition
* foreachPartition() -> Applies a function f to each partition of this RDD.
*/
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Operations on Per-Partition basis").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		
		val data = sc.parallelize(List(1,2,3,4,5,6,7,8,9), 3)
		data.foreachPartition(x => println(x.reduce((v,v1) => v + v1)))

		val res = data.mapPartitions(x => List(x.next).iterator).collect
		println(res.mkString(","))

		val data1 = sc.parallelize(List("a","b","c","d","e","f"), 2)
		val res1 = data1.mapPartitionsWithIndex(myFunc).collect
		res1.foreach(println)

		sc.stop
		spark.stop
	}

	def myFunc(index: Int, iter: Iterator[String]): Iterator[String]= {
		iter.toList.map(x => "[PartID: " + index + ", value: " + x + "]").iterator
	}
}
