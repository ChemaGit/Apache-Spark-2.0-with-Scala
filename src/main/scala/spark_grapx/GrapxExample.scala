package spark_grapx

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.sql._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

object GrapxExample {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder.appName("Grapx Example").master("local").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		val vertexArray = Array(
			(1L,("Alice",28)),(2L,("Bob",27)),(3L,("Charlie",65)),(4L,("David",42)),(5L,("Ed",55)),(6L,("Fran",50))
		)
		val edgeArray = Array(
			Edge(2L,1L,7),Edge(2L,4L,2),Edge(3L,2L,4),Edge(3L,6L,3),Edge(4L,1L,1),Edge(5L,2L,2),Edge(5L,3L,8),Edge(5L,6L,3)
		)
		val vertexRDD: RDD[(Long,(String,Int))] = sc.parallelize(vertexArray)
		val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
		val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

		graph.vertices.filter{case(id,(name,age)) => age > 30}.collect.foreach{case(id, (name, age)) => println(s"$name is $age")}

    println("*********************************")

		for(triplet <- graph.triplets.collect) {
			println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
		}

    println("***********************************")

		case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
		val initialUserGraph: Graph[User, Int] = graph.mapVertices{case(id,(name, age)) => User(name, age, 0, 0)}
		val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees){case(id,u,inDegOpt) => User(u.name,u.age,inDegOpt.getOrElse(0),u.outDeg)}
		.outerJoinVertices(initialUserGraph.outDegrees){case(id,u,outDegOpt) => User(u.name,u.age,u.inDeg,outDegOpt.getOrElse(0))}

		for( (id, property) <- userGraph.vertices.collect){
			println(s"User $id is called ${property.name} and is liked by ${property.inDeg} people.")
		}

		sc.stop()
		spark.stop()
	}
}
