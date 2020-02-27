package exercises_cert_1

/** Question 21
  * Problem Scenario 46 : You have been given belwo list in scala (name,sex,cost) for each work done.
  * List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000),
  * ("Deepika" , "female",2000),("Deepak" , "female", 2000),
  * ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000))
  * Now write a Spark program to load this list as an RDD and do the sum of cost for combination of name and sex (as key)
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._

object exercise_4 {

  val spark = SparkSession
    .builder()
    .appName("exercise_4")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_4")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val emp = sc
        .parallelize(List( ("Deeapak" , "male", 4000), ("Deepak" , "male", 2000), ("Deepika" , "female",2000),("Deepak" , "female", 2000), ("Deepak" , "male", 1000) , ("Neeta" , "female", 2000)))

      val empKey = emp
        .map({case(name,sex,cost) => ( (name,sex),cost)})

      val cost = empKey
        .reduceByKey( (v, v1) => v + v1)

      cost
        .collect
        .foreach(println)
      /*
      ((Deepak,female),2000)
      ((Neeta,female),2000)
      ((Deeapak,male),4000)
      ((Deepak,male),3000)
      ((Deepika,female),2000)
       */
      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}

