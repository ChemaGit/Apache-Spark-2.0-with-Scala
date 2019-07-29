// Creation of SparkContext object
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object CreatingRDDs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local")setAppName("MyApp")
    val sc = new SparkContext(conf)

    // Creating RDDs
    val lines = sc.parallelize(List("panda", "I like Pandas"))
    val linesText = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/product.csv")
    linesText.collect.foreach(println)

    val data = sc.parallelize(Seq( ("maths", 52),("english",75),("science",82),("computer",65),("maths",85)))
    val sorted = data.sortByKey()
    sorted.foreach(println)

    sc.stop()

    val spark = SparkSession.builder.appName("MyFirstApp").master("local").getOrCreate()
	  val dataRDD = spark.read.textFile("hdfs://quickstart.cloudera/user/cloudera/files/file_4.txt").rdd
	  dataRDD.collect.foreach(println)

	  val words = dataRDD.flatMap(line => line.split(" ")).map(w => (w, 1)).reduceByKey( (v, c) => v + c )
	  words.sortBy(t => t._2,false).collect.foreach(println)

    spark.stop()
  }
}



