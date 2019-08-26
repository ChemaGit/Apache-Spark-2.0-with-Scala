
  package exercises_cert_1

  /** Question 28
    * Problem Scenario 43 : You have been given following code snippet.
    * val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )
    * val flattened = grouped.flatMap {A => groupValues.map { value => B } }
    * You need to generate following output.
    * Hence replace A and B
    * Array((1,two,3,4),(1,two,5,6))
    */

    import org.apache.spark.sql._

    object exercise_7 {
      def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().appName("exercise 7").master("local").getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("ERROR")
        val grouped = sc.parallelize( Seq( ( (1,"two"), List((3,4), (5,6)) ) ) )
        val flattened = grouped.flatMap({case(s,groupValues) => groupValues.map{value => (s._1, s._2,value._1, value._2)}})
        flattened.collect.foreach(x => println(x))
        // res2: Array[(Int, String, Int, Int)] = Array((1,two,3,4), (1,two,5,6))
        sc.stop()
        spark.stop()
      }
    }
