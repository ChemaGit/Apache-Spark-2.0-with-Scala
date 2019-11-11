package spark_streaming

import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RequestCount {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Request Count").master("local[*]").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val ssc = new StreamingContext(sc, Seconds(2))
		val mystream = ssc.socketTextStream("quickstart.cloudera",4444)
		val userreqs = mystream.map(line => (line.split(' ')(2),1)).reduceByKey( (x, y) => x + y)

		userreqs.print()		
		
		val sortedreqs = userreqs.map(pair => pair.swap).transform(rdd => rdd.sortByKey(false))
		sortedreqs.foreachRDD((rdd, time) => {
			println("Top ips @ " + time)
			rdd
        .take(5)
        .foreach(pair => println("Ips: %s (%s)\n".format(pair._2, pair._1)))
		})

		/* ----- Example 3: total counts for all users over time ----- */
		// checkpointing must be enabled for state operations
		ssc.checkpoint("checkpoints")

		def updateCount = (newCounts: Seq[Int], state: Option[Int]) => {
			// sum the new counts for the key (user)
			val newCount = newCounts.foldLeft(0)(_ + _)

			// get the previous count for the current user
			val previousCount = state.getOrElse(0)

			//the new state for the user is the old count plus the new count
			Some(newCount + previousCount)
		}

		val totalUserreqs = userreqs.updateStateByKey(updateCount)

		totalUserreqs.foreachRDD(rdd => {
			println("Total users: " + rdd.count())
			rdd.take(5).foreach(println)
		}
		)

		/* ----- Example 4: Display top 5 users over 30 second window, output every 6 seconds  ----- */

		val reqcountsByWindow = mystream
      .map(line => (line.split(' ')(2),1))
      .reduceByKeyAndWindow((x: Int,y: Int) => x+y, Seconds(30),Seconds(6))

		val topreqsByWindow=reqcountsByWindow
      .map(pair => pair.swap)
      .transform(rdd => rdd.sortByKey(false))

		topreqsByWindow.foreachRDD(rdd => {
			println("Top users by window:")
			rdd
        .take(5)
        .foreach(pair => printf("User: %s (%s)\n",pair._2, pair._1))
		})


		// after setup, start the stream
		ssc.start()
		ssc.awaitTermination()
	}
}
