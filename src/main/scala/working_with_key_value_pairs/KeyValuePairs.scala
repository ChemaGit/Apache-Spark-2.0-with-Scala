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
		val rdd1 = sc.parallelize(List("Alice","Bob","Joe")).map(a => (a, 1))
		val rdd2 = sc.parallelize(List("John","Alice","Daniel")).map(a => (a, 1))
		val join = rdd1.join(rdd2).collect
		join.foreach(println)
		val lOuterJoin = rdd1.leftOuterJoin(rdd2).collect
		lOuterJoin.foreach(println)
		val rOuterJoin = rdd2.rightOuterJoin(rdd2).collect
		rOuterJoin.foreach(println)

		sc.stop()
		spark.stop()
	}
}
