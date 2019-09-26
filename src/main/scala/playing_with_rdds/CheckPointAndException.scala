package playing_with_rdds

import org.apache.spark.sql._
import java.lang.Exception
import java.lang.RuntimeException

object CheckPointAndException {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("CheckPoint and Exception").master("local[*]").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		sc.setCheckpointDir("file:///tmp/checkPointDir")

		try {
			val rddEnteros = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
			for(i <- 1 to 1001) {
				val sumRddEnteros = rddEnteros.map(x => x + 1)
				if(i % 10 == 0) {
					println(s"Bucle numero $i")
					// Before any actions "checkpoint"
					sumRddEnteros.checkpoint()
					println(s"sumRddEnteros.count() ${sumRddEnteros.count()}")
				}
			}
		} catch {
			case ex: Exception => println(s"Exception: ${ex.toString()}")
			case ex: RuntimeException => println(s"Exception: ${ex.toString()}")
		} finally {
			println("See you....")
		}

		sc.stop()
		spark.stop()
	}
}
