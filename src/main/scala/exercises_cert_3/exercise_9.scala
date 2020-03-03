package exercises_cert_3

/** Question 59
  * Problem Scenario 84 : In Continuation of previous question, please accomplish following activities.
  * 1. Select all the products which has product code as null
  * 2. Select all the products, whose name starts with Pen and results should be order by Price descending order.
  * 3. Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order.
  * 4. Select top 2 products by price
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object exercise_9 {

  val spark = SparkSession
    .builder()
    .appName("exercise_9")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_9")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      import spark.implicits._

      val products = sc
        .textFile(s"${path}product.csv")
        .map(line => line.split(","))
        .filter(r => r(0).equals("productID") == false)
        .map(r => (r(0).toInt,r(1),r(2),r(3).toInt,r(4).toDouble))
        .toDF("id","code","name","quantity","price")
        .cache()

      products.createOrReplaceTempView("products")
      // 1. Select all the products which has product code as null
      sqlContext
        .sql(
          """SELECT *
            |FROM products
            |WHERE code IS NULL""".stripMargin)
        .show()

      // 2. Select all the products, whose name starts with Pen and results should be order by Price descending order.
      sqlContext
        .sql(
          """SELECT *
            | FROM products
            | WHERE name LIKE("Pen%")
            | ORDER BY price DESC""".stripMargin)
        .show()

      // 3. Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order.
      sqlContext
        .sql(
          """SELECT *
            |FROM products
            |WHERE name LIKE("Pen%")
            |ORDER BY price DESC, quantity ASC""".stripMargin)
        .show()

      // 4. Select top 2 products by price
      sqlContext
        .sql(
          """SELECT *
            |FROM products
            |ORDER BY price DESC
            |LIMIT 2""".stripMargin)
        .show()

      products.unpersist()

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
