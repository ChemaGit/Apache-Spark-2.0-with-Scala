package exercises_cert_6

import org.apache.spark.sql.SparkSession

/**
  * /**
  * Problem 6: Provide two solutions for steps 2 to 7
  * Using HIVE QL over Hive Context
  * Using Spark SQL over Spark SQL Context or by using RDDs
  * 1. create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store.
  * 2. On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]
  * 3. Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
  * 4. find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
  * 5. On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
  * 6. On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
  * 7. Store the result of 5 and 6 in new meta store tables within hive. [this proves your ability to use metastore as a sink]
  **/
  *
  * 1. create a hive meta store database named problem6 and import all tables from mysql retail_db database into hive meta store.
  * $ beeline -u jdbc:hive2://quickstart.cloudera:10000
  * hive> CREATE DATABASE problem6;
  *
  $ sqoop import-all-tables \
    --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
    --username retail_dba \
    --password cloudera \
    --as-textfile \
    --hive-import \
    --hive-database problem6 \
    --create-hive-table \
    --outdir /home/cloudera/outdir \
    --bindir /home/cloudera/bindir \
    --autoreset-to-one-mapper
  */
object exercise_9 {

  val spark = SparkSession
    .builder()
    .appName("exercise_9")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "HiveETL")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("ERROR")

    try {
      // 2. On spark shell use data available on meta store as source and perform step 3,4,5 and 6. [this proves your ability to use meta store as a source]
      sqlContext.sql("SHOW DATABASES")
      sqlContext.sql("""USE problem6""")
      // 3. Rank products within department by price and order by department ascending and rank descending [this proves you can produce ranked and sorted data on joined data sets]
      val hiveResult = sqlContext
          .sql(
            """SELECT d.department_id, p.product_id,p.product_name,p.product_price,
              |RANK() OVER(PARTITION BY d.department_id ORDER BY p.product_price) AS product_price_rank,
              |DENSE_RANK() OVER(PARTITION BY d.department_id ORDER BY p.product_price) AS product_dense_price_rank
              |FROM products p INNER JOIN categories c ON(c.category_id = p.product_category_id) INNER JOIN departments d ON(c.category_department_id = d.department_id)
              |ORDER BY d.department_id, product_price_rank DESC,product_dense_price_rank
            """.stripMargin)
          .cache()
      hiveResult.show()

      // 4. find top 10 customers with most unique product purchases. if more than one customer has the same number of product purchases then the
      // customer with the lowest customer_id will take precedence [this proves you can produce aggregate statistics on joined datasets]
      val hiveResult2 = sqlContext
          .sql(
            """SELECT c.customer_id, c.customer_fname, COUNT(DISTINCT(oi.order_item_product_id)) AS unique_product
              |FROM customers c INNER JOIN orders o ON(c.customer_id = o.order_customer_id) INNER JOIN order_items oi ON(o.order_id = oi.order_item_order_id)
              |GROUP BY c.customer_id, c.customer_fname,
              |ORDER BY unique_products DESC, c.customer_id
              |LIMIT 10
            """.stripMargin)
          .cache()
      hiveResult2.show()

      // 5. On dataset from step 3, apply filter such that only products less than 100 are extracted [this proves you can use subqueries and also filter data]
      val hiveResult3 = hiveResult
          .where("product_price < 100")
          .cache()
      hiveResult3.show()

      // 6. On dataset from step 4, extract details of products purchased by top 10 customers which are priced at less than 100 USD per unit [this proves you can use subqueries and also filter data]
      hiveResult2.createOrReplaceTempView("dataset4")
      val hiveResult4 = sqlContext
          .sql(
            """SELECT DISTINCT(p.*)
              |FROM products p JOIN order_items oi ON(oi.order_item_product_id = p.product_id) JOIN orders o ON(o.order_id = oi.order_item_order_id) JOIN dataset4 dt4 ON(o.order_customer_id = dt4.customer_id)
              |WHERE p.product_price < 100
            """.stripMargin)
          .cache()

      // 7. Store the result of 5 and 6 in new meta store tables within hive. [this proves your ability to use metastore as a sink]
      hiveResult3
        .write
        .parquet("/user/hive/warehouse/problem6.db/hive_result_3")
      hiveResult4
        .write
        .parquet("/user/hive/warehouse/problem6.db/hive_result_4")

      sqlContext
        .sql(
          """CREATE TABLE hive_result_3(
            |product_id INT,
            |product_name STRING,
            |product_price DOUBLE,
            |product_price_rank INT,
            |product_dense_rank INT)
            |STORED AS PARQUET
            |LOCATION "/user/hive/warehouse/problem6.db/hive_result_3" """.stripMargin)
      sqlContext.sql("""SELECT * FROM hive_result_3""").show()

      sqlContext
        .sql(
          """CREATE TABLE hive_result_4(
            |product_id INT,
            |product_category_id INT,
            |product_name STRING,
            |product_description STRING,
            |product_price DOUBLE,
            |product_image STRING)
            |STORED AS PARQUET
            |LOCATION "/user/hive/warehouse/problem6.db/hive_result_4" """.stripMargin)
      sqlContext.sql("""SELECT * FROM hive_result_4""").show()

      /*
      $ beeline -u jdbc:hive2://quickstart.cloudera:10000/problem6
      hive> show tables;
      hive> SELECT * FROM hive_result_3;
      hive> SELECT * FROM hive_result_4;
      hive> !q
       */


      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped")
      spark.stop()
      println("SparkSession stopped")
    }

  }

}
