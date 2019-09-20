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

    val products = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/product.csv")
      .map(line => line.split(","))
      .filter(arr => arr(0) != "productID")
      .map(arr => (arr(0).toInt,arr(1),arr(2),arr(3).toInt,arr(4).toDouble,arr(5).toInt))

    val df = products.toDF("id","code","name","quantity","price","idSupplier")
    df.createOrReplaceTempView("products_view")

    //df.rdd.repartition(1).saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/tables/products")
    //spark.sqlContext.sql("""CREATE EXTERNAL TABLE  IF NOT EXISTS t_products(id int,code string,name string,quantity int,price double,idSupplier int) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS TEXTFILE LOCATION "hdfs://quickstart.cloudera/user/cloudera/tables/products" """)

    spark.sqlContext.sql("show tables").show()

    spark.sqlContext.sql("""SELECT * FROM t_products""").show()
    spark.sqlContext.sql("""SELECT * FROM products_view""").show()

    // 1. Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
    spark.sqlContext.sql("""SELECT id AS ID, code AS Code, name AS Description,quantity, price FROM t_products""").show()
    // 2. Select code and name both separated by '-' and header name should be 'Product Description'.
    spark.sqlContext.sql("""SELECT concat(code,"-",name) AS `Product Description` FROM t_products""").show()
    // 3. Select all distinct prices.
    spark.sqlContext.sql("""SELECT DISTINCT(price) FROM t_products""").show()
    // 4. Select distinct price and name combination.
    spark.sqlContext.sql("""SELECT DISTINCT(price), name FROM t_products""").show()
    // 5. Select all price data sorted by both code and productID combination.
    spark.sqlContext.sql("""SELECT id,code, price FROM t_products SORT BY code, id""").show()
    // 6. count number of products.
    spark.sqlContext.sql("""SELECT COUNT(*) AS num_products from t_products""")
    // 7. Count number of products for each code.
    spark.sql("""SELECT code, COUNT(*) FROM t_products GROUP BY code""").show()

    // spark.sqlContext.sql("""SELECT * FROM src1 LIMIT 50""").show()
    sc.stop()
    spark.stop()
  }

}
