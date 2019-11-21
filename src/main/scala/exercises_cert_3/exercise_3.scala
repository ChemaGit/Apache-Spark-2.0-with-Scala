package exercises_cert_3

import org.apache.spark.sql.SparkSession

/** Question 52
  * hive table t_product_parquet
  * id   code    name          quantity  price
  * 1001	PEN	Pen Red	        5000	1.23
  * 1002	PEN	Pen Blue	8000	1.25
  * 1003	PEN	Pen Black	2000	1.25
  * 1004	PEC	Pencil 2B	10000	0.48
  * 1005	PEC	Pencil 2H	8000	0.49
  * 1006	PEC	Pencil HB	0	9999.99
  * 2001	PEC	Pencil 3B	500	0.52
  * 2002	PEC	Pencil 4B	200	0.62
  * 2003	PEC	Pencil 5B	100	0.73
  * 2004	PEC	Pencil 6B	500	0.47
  * Problem Scenario 86 : In Continuation of previous question, please accomplish following activities.
  * 1. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity.
  * 2. Select minimum and maximum price for each product code.
  * 3. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity for each product code, however make sure Average and Standard deviation will have maximum two decimal values.
  * 4. Select all the product code and average price only where product count is more than or equal to 3.
  * 5. Select maximum, minimum , average price and total quantity of all the products for each code. Also produce the same across all the products.
  */

object exercise_3 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 3")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val productsDF = sc
      .textFile("hdfs://quickstart.cloudera/user/cloudera/files/product.csv")
      .map(line => line.split(","))
      .filter(r => r(0) != "productID")
      .map(r => (r(0).toInt,r(1),r(2),r(3).toInt,r(4).toDouble,r(5).toInt))
      .toDF("id","code","name","quantity","price","id_supplier")

    productsDF.createOrReplaceTempView("t_products")

    // 1. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity.
    spark.sqlContext.sql("""SELECT MAX(price) AS max_price,MIN(price) AS min_price,ROUND(AVG(price),2) AS avg_price, ROUND(STDDEV(price),2) AS sdt_dev_price,SUM(quantity) AS total_quantity FROM t_products""").show()

    // 2. Select minimum and maximum price for each product code.
    spark.sqlContext.sql("""SELECT code, MAX(price) AS max_price,MIN(price) AS min_price FROM t_products GROUP BY code""").show()

    // 3. Select Maximum, minimum, average , Standard Deviation for price column, and total quantity for each product code, however make sure Average and Standard deviation will have maximum two decimal values.
    spark.sqlContext.sql("""SELECT code,MAX(price) AS max_price,MIN(price) AS min_price,ROUND(AVG(price),2) AS avg_price, ROUND(STDDEV(price),2) AS sdt_dev_price,SUM(quantity) AS total_quantity FROM t_products GROUP BY code""").show()

    // 4. Select all the product code and average price only where product count is more than or equal to 3.
    spark.sqlContext.sql("""SELECT COUNT(code) AS prod_by_code,code, ROUND(AVG(price),2) AS avg_price FROM t_products GROUP BY code HAVING COUNT(code) >= 3""").show()

    // 5. Select maximum, minimum , average price and total quantity of all the products for each code. Also produce the same across all the products.
    spark.sqlContext.sql("""SELECT code, MAX(price) AS max_price,MIN(price) AS min_price,ROUND(AVG(price),2) AS avg_price,SUM(quantity) AS total_quantity FROM t_products GROUP BY code WITH ROLLUP""").show()

    sc.stop()
    spark.stop()
  }
}
