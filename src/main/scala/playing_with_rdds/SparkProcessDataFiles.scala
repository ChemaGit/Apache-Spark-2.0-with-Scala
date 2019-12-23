package playing_with_rdds

import org.apache.spark.sql._
import scala.xml._

object SparkProcessDataFiles {
	// Given a string containing XML, parse the string, and
	// return an iterator of activation XML records (Nodes) contained in the string
	def getActivations(xmlstring: String): Iterator[Node] = {
		val nodes = XML.loadString(xmlstring) \\ "activation"
		nodes.toIterator
	}

	// Given an activation record (XML Node), return the model name
	def getModel(activation: Node): String = {
		(activation \ "model").text
	}

	// Given an activation record (XML Node), return the account number
	def getAccount(activation: Node): String = {
		(activation \ "account-number").text
	}
	def main(args: Array[String]) {
		val spark = SparkSession.builder.appName("Spark Process Data Files").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val rdd = sc.wholeTextFiles("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/activations")
		val fRdd = rdd.flatMap(pair => getActivations(pair._2))
		val format = fRdd.map(xml => "%s-%s".format(getAccount(xml), getModel(xml)))
		//format.collect.foreach(println)		
		format.saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/results/account-models")

		sc.stop()
		spark.stop()
	}
}
