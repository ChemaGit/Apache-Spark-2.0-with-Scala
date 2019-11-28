package working_with_key_value_pairs

import org.apache.spark.sql.SparkSession

object DistrubutedKeyValuePairs {

  /**
    * Distributed Key-Value Pairs ==> Pair RDDs
    * In single-node Scala, key-value pairs can be thought of as maps.
    * Most common in world of big data processing:
    * Operating on data in the form of key-value pairs.
    *
    * We realized that most of our computations involved applying a map
    * operation to each logical "record" in our input in order to
    * compute a set of intermediate key/value pairs, and then
    * applying a reduce operation to all the values that shared
    * the same key, in order to combine the derived data a
    * appropriately.
    *
    * Example: In a JSON record, it may be desirable to create an RDD of properties of type:
    * RDD[(String, Property)] where String is a key representing a value, and Property the
    * properties of that value.
    */
  case class Property(street: String, city: String, state: String)
  //Where instances of Properties can be grouped by their respective cities and
  //represented in a RDD of key-value pairs.

  def main(args: Array[String]) {
    val spark = SparkSession
        .builder()
        .appName("Distributed Key-Value Pairs")
        .master("local[*]")
        .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /**
      * Pair RDDs allow you to act on each key in parallel or
      * regroup data across the network.
      * RDD[(K, V)] // <== treated specially by Spark!
      *
      * Some of the most important extension methods for RDDs containing pairs are:
      * def groupByKey(): RDD[(K, Iterable[V])]
      * def reduceByKey(func: (V, V) => V): RDD[(K, V)]
      * def join[W](other: RDD[(K, W)]): RDD[(K, (V, W))]
      *
      * Creating a Pair RDD
      */
    val largeList = List(('a', 2),('a', 5),('z', 7),('x', 1),('c', 2),('b', 2))
    val rdd = sc.parallelize(largeList) //Has type: org.apache.spark.rdd.RDD[(Char, Int)]
    val pairRdd = rdd.map(t => (t._1, t))
    /**
      * Once created, we can now use transformations specific to key-value pairs
      * such as reduceByKey, groupByKey, and join.
      */
    val reduce = pairRdd.reduceByKey( (c, v) => (c._1,c._2 + v._2))
    reduce.foreach(println)

    sc.stop()
    spark.stop()
  }

}
