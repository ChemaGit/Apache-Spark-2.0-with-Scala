package exercises_cert_2

import org.apache.spark.sql._

/** Question 42
  * Problem Scenario 85 : In Continuation of previous question, please accomplish following activities.
  * 1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
  * 2. Select code and name both separated by '-' and header name should be 'Product Description'.
  * 3. Select all distinct prices.
  * 4. Select distinct price and name combination.
  * 5. Select all price data sorted by both code and productID combination.
  * 6. count number of products.
  * 7. Count number of products for each code.
  */

object exercise_4 {

  def main(args: Array[String]): Unit = {
    val warehouseLocation = "/user/hive/warehouse"
    val spark = SparkSession.builder()
      .appName("exercise 4")
      .master("local")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    spark.sqlContext.sql("show tables").show()
    spark.sqlContext.sql("select * from src1 limit 10").show()

    sc.stop()
    spark.stop()
  }

}
