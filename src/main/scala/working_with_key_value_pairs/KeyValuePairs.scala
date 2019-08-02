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
		println("**JOIN**")
		join.foreach(println)
		val lOuterJoin = rdd1.leftOuterJoin(rdd2).collect
		println("**LEFT OUTER JOIN**")
		lOuterJoin.foreach(println)
		val rOuterJoin = rdd1.rightOuterJoin(rdd2).collect
		println("**RIGHT OUTER JOIN**")
		rOuterJoin.foreach(println)

		println("**************COMBINEBYKEY FUNCTIONS***********************")				
		val rdd3 = sc.parallelize(List( ("A",3), ("A",9), ("A",12), ("A",5), ("B",4), ("B",10), ("B",11), ("B",20), ("B",25), ("C",32), ("C",91), ("C",122), ("C",3), ("C",55)),2)
		val cByKey = rdd3.combineByKey( ((c: Int) => c + 0) ,((c: Int, v: Int) => c + v) ,((v: Int, v1: Int) => v + v1) ).collect
		cByKey.foreach(println)

		println("**************COUNTBYKEY FUNCTIONS***********************")				
		val rdd4 = sc.parallelize(List((1,2),(3,4),(3,6)))
		val countByKey = rdd4.countByKey
		countByKey.foreach(println)

		println("**************COLLECTASMAP FUNCTIONS***********************")				
		val collectAsMap = rdd4.collectAsMap
		collectAsMap.foreach(println)

		println("**************LOOKUP FUNCTIONS***********************")				
		val lookUp = rdd4.lookup(3)
		lookUp.foreach(println)

		sc.stop()
		spark.stop()
	}
}
