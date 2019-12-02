package exercises_cert_3

import org.apache.spark.sql.SparkSession


/** Question 59
  * Problem Scenario 84 : In Continuation of previous question, please accomplish following activities.
  * 1. Select all the products which has product code as null
  * 2. Select all the products, whose name starts with Pen and results should be order by Price descending order.
  * 3. Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order.
  * 4. Select top 2 products by price
  */

object exercise_9 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 9")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val products = sc
      .textFile("hdfs://quickstart.cloudera/user/cloudera/files/product.csv")
      .map(line => line.split(","))
      .filter(r => !r(0).equals("productID"))
      .map(r => (r(0).toInt,r(1),r(2),r(3).toInt,r(4).toDouble))
      .toDF("id","code","name","quantity","price")

    products.createOrReplaceTempView("products")
    // 1. Select all the products which has product code as null
    spark.sqlContext.sql("""SELECT * FROM products WHERE code IS NULL""").show()
    // 2. Select all the products, whose name starts with Pen and results should be order by Price descending order.
    spark.sqlContext.sql("""SELECT * FROM products WHERE name LIKE("Pen%") ORDER BY price DESC""").show()
    // 3. Select all the products, whose name starts with Pen and results should be order by Price descending order and quantity ascending order.
    spark.sqlContext.sql("""SELECT * FROM products WHERE name LIKE("Pen%") ORDER BY price DESC, quantity ASC""").show()
    // 4. Select top 2 products by price
    spark.sqlContext.sql("""SELECT * FROM products ORDER BY price DESC LIMIT 2""").show()

    sc.stop()
    spark.stop()
  }

}
