package advanced_spark_programming

import org.apache.spark.sql.SparkSession

object Accumulators {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Accumulators").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val accum = sc.accumulator(0, "My Accumulator")
		sc.parallelize(List(1,2,3,4)).foreach(x => accum += x)
		println("My Accumulator: " + accum.value) 

		val file = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/MergedEmployee.txt")
		val blankLines = sc.accumulator(0, "CDRAcc")
		val callSigns = file.flatMap(line => {
			if(line == ""){
				blankLines += 1
			}
			line.split(",")
		})		
		println("CDRAcc: " + blankLines.value)

		callSigns.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/callSigns")
		// it needs an action to print the correct value
		println("CDRAcc again: " + blankLines.value)

		sc.stop()
		spark.stop()
	}
}
