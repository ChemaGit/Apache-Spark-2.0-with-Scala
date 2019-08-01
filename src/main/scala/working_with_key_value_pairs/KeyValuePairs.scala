package working_with_key_value_pairs

import org.apache.spark.sql.SparkSession

object KeyValuePairs {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("KeyValuePairs").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")
		println("**************AGGREGATE FUNCTIONS***********************")
		val wordRdd = sc.parallelize(Array("A","V","C","A","C","C","D","D","E","V","A","E")).map(w => (w, 1))		
		val wordPairRdd = wordRdd.aggregateByKey(0)(( (i,v) => i + v ) ,( (v,c) => v + c ) )
		wordPairRdd.foreach(println)

		println("**************JOIN FUNCTIONS***********************")				

		sc.stop()
		spark.stop()
	}
}
