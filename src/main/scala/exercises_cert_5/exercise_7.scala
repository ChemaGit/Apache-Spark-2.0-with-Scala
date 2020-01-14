package exercises_cert_5

import org.apache.spark.sql.SparkSession

/** Question 90
  * Problem Scenario 75 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Please accomplish following activities.
  * 1. Copy "retail_db.order_items" table to hdfs in respective directory question90/order_items .
  * 2. Do the summation of entire revenue in this table using scala
  * 3. Find the maximum and minimum revenue as well.
  * 4. Calculate average revenue
  * Columns of orde_items table : (order_item_id , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Save results in one file, in directory /user/cloudera/question90
  *
  * $ sqoop import \
  * --connect jdbc:mysql://quickstart.cloudera/retail_db \
  * --username retail_dba \
  * --password cloudera \
  * --table order_items \
  * --as-textfile \
  * --delete-target-dir \
  * --target-dir /user/cloudera/tables/order_items \
  * --bindir /home/cloudera/bindir \
  * --outdir /home/cloudera/outdir
  *
  * $ hdfs dfs -ls /user/cloudera/tables/order_items/
  * $ hdfs dfs -cat /user/cloudera/tables/order_items/part* | head -n 20
  */

object exercise_7 {

  val spark = SparkSession
    .builder()
    .appName("exercise 7")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    /**
      * SPARK RDD
      */
    val order_items_rdd = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/tables/order_items/")
        .map(line => line.split(","))
        .map(arr => arr(4).toDouble)
        .persist()

    val sumTotalRevenue = order_items_rdd.sum()
    val maxRevenue = order_items_rdd.max()
    val minRevenue = order_items_rdd.min()
    val averageRevenue = order_items_rdd.sum() / order_items_rdd.count()

    println(s"Total revenue: $sumTotalRevenue")
    println(s"Max Revenue: $maxRevenue")
    println(s"Min Revenue: $minRevenue")
    println(s"Average Revenue: $averageRevenue")

    sc
      .parallelize(List(s"Total revenue: $sumTotalRevenue", s"Max Revenue: $maxRevenue",s"Min Revenue: $minRevenue",s"Average Revenue: $averageRevenue"))
      .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercises/question_90/rdd")

    /**
      * SPARK DATAFRAMES
      */

    import spark.implicits._

    val order_items_df = order_items_rdd.toDF("revenue")
    order_items_df.createOrReplaceTempView("t_revenue")

    spark
        .sqlContext
        .sql("""SELECT ROUND(SUM(revenue),2) AS SumRevenue, MAX(revenue) AS MaxRevenue, MIN(revenue) AS MinRevenue, ROUND(AVG(revenue),2) AS AverageRevenue FROM t_revenue""")
        .rdd
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercises/question_90/dataframes")

    sc.stop()
    spark.stop()
  }

}
