package exercises_cert_6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
Problem 1:
1.Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression
2.Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy compression
3.Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders-items items as dataframes.
4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day.
The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending.
Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
5.Store the result as parquet file into hdfs using gzip compression under folder
/user/cloudera/problem1/result4a-gzip
/user/cloudera/problem1/result4b-gzip
/user/cloudera/problem1/result4c-gzip
6.Store the result as parquet file into hdfs using snappy compression under folder
/user/cloudera/problem1/result4a-snappy
/user/cloudera/problem1/result4b-snappy
/user/cloudera/problem1/result4c-snappy
7.Store the result as CSV file into hdfs using No compression under folder
/user/cloudera/problem1/result4a-csv
/user/cloudera/problem1/result4b-csv
/user/cloudera/problem1/result4c-csv
8.create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result

  1.Using sqoop, import orders table into hdfs to folders /user/cloudera/problem1/orders. File should be loaded as Avro File and use snappy compression
$ sqoop import \
  --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username retail_dba \
  --password cloudera \
  --table orders \
  --as-avrodatafile \
  --compress \
  --compression-codec snappy \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/orders \
  --outdir /home/cloudera/outdir \
  --bindir /home/cloudera/bindir

$ hdfs dfs -ls /user/cloudera/problem1/orders
$ hdfs dfs -text  /user/cloudera/problem1/orders/part-m-00000.avro | head -n 10
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/orders/part-m-00000.avro

  2.Using sqoop, import order_items  table into hdfs to folders /user/cloudera/problem1/order-items. Files should be loaded as avro file and use snappy compression
$ sqoop import \
  --connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
  --username retail_dba \
  --password cloudera \
  --table order_items \
  --as-avrodatafile \
  --compress \
  --compression-codec snappy \
  --delete-target-dir \
  --target-dir /user/cloudera/problem1/order-items \
  --outdir /home/cloudera/outdir \
  --bindir /home/cloudera/bindir

$ hdfs dfs -ls /user/cloudera/problem1/order-items
$ hdfs dfs -text /user/cloudera/problem1/order-items/part-m-00000.avro | head -n 10
$ avro-tools getmeta hdfs://quickstart.cloudera/user/cloudera/problem1/order-items/part-m-00000.avro

  */

object exercise_3 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 3")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    // 3.Using Spark Scala load data at /user/cloudera/problem1/orders and /user/cloudera/problem1/orders-items items as dataframes.
    import com.databricks.spark.avro._

    val ordersDF = spark
        .sqlContext
        .read
        .avro("hdfs://quickstart.cloudera/user/cloudera/problem1/orders/")

    val orderItemsDF = spark
        .sqlContext
        .read
        .avro("hdfs://quickstart.cloudera/user/cloudera/problem1/order-items/")

    import spark.implicits._

    // 4.Expected Intermediate Result: Order_Date , Order_status, total_orders, total_amount. In plain english, please find total orders and total amount per status per day.
    //  The result should be sorted by order date in descending, order status in ascending and total amount in descending and total orders in ascending.
    // Aggregation should be done using below methods. However, sorting can be done using a dataframe or RDD. Perform aggregation in each of the following ways
    //  a). Just by using Data Frames API - here order_date should be YYYY-MM-DD format
    val joined = ordersDF
        .join(orderItemsDF,$"order_id" === $"order_item_order_id", "inner")
        .persist()

    val resultDF = joined
        .groupBy(col("order_date"),col("order_status"))
        .agg(countDistinct("order_id").as("total_orders"),round(sum("order_item_subtotal"),2).as("total_amount"))
        .selectExpr("""from_unixtime(order_date / 1000,"yyyy-MM-dd") as order_date""", """order_status""", """total_orders""", """total_amount""")
        .orderBy(col("order_date").desc, col("order_status").asc,col("total_amount").desc,col("total_orders").asc)

    //  b). Using Spark SQL  - here order_date should be YYYY-MM-DD format
    joined.createOrReplaceTempView("joined")
    val resultSQL = spark
        .sqlContext
        .sql("""SELECT from_unixtime(order_date / 1000, "yyyy-MM-dd") AS order_date, order_status, COUNT(DISTINCT(order_id)) AS total_orders, ROUND(SUM(order_item_subtotal),2) AS total_amount FROM joined GROUP BY order_date, order_status ORDER BY order_date DESC,order_status ASC,total_amount DESC,total_orders ASC""")

    //  c). By using combineByKey function on RDDS -- No need of formatting order_date or total_amount
    val resultRDD = joined
        .rdd
        .map(r => ( (r(1).toString.toLong, r(3).toString), (r(0).toString.toInt, r(8).toString.toDouble)))
        .combineByKey( ( (v:(Int,Double)) => (Set(v._1), v._2) ), ( (c:(Set[Int],Double),v:(Int,Double)) => (c._1 + v._1,c._2 + v._2)), ( (c:(Set[Int],Double),v:(Set[Int],Double)) => ( c._1 ++ v._1, c._2 + v._2)))
        .map({ case(((d, s), (to, ta))) => (d,s,to.size,ta)})
        .toDF("order_date","order_status","total_orders","total_amount")
        .selectExpr("""from_unixtime(order_date / 1000,"yyyy-MM-dd") as order_date""", """order_status""", """total_orders""", """ROUND(total_amount,2) AS total_amount""")
        .orderBy(col("order_date").desc, col("order_status").asc,col("total_amount").desc,col("total_orders").asc)

    // 5.Store the result as parquet file into hdfs using gzip compression under folder
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","gzip")
    // /user/cloudera/problem1/result4a-gzip
    resultDF.write.parquet("hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-gzip")
    // /user/cloudera/problem1/result4b-gzip
    resultSQL.write.parquet("hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-gzip")
    // /user/cloudera/problem1/result4c-gzip
    resultRDD.write.parquet("hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-gzip")

    // 6.Store the result as parquet file into hdfs using snappy compression under folder
    spark
        .sqlContext
        .setConf("spark.sql.parquet.compression.codec","snappy")
    // /user/cloudera/problem1/result4a-snappy
    resultDF.write.parquet("hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-snappy")
    // /user/cloudera/problem1/result4b-snappy
    resultSQL.write.parquet("hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-snappy")
    // /user/cloudera/problem1/result4c-snappy
    resultRDD.write.parquet("hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-snappy")

    // 7.Store the result as CSV file into hdfs using No compression under folder
    // /user/cloudera/problem1/result4a-csv
    resultDF
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/problem1/result4a-csv")
    // /user/cloudera/problem1/result4b-csv
    resultSQL
        .rdd
        .map(r => r.mkString(","))
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/problem1/result4b-csv")
    // /user/cloudera/problem1/result4c-csv
    resultRDD
      .rdd
      .map(r => r.mkString(","))
      .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/problem1/result4c-csv")

    // 8.create a mysql table named result and load data from /user/cloudera/problem1/result4a-csv to mysql table named result
    /*
      mysql -u root -p cloudera
      use retail_export
      CREATE TABLE result(order_date varchar(16),order_status varchar(16),total_orders int,total_amount double);

      sqoop export \
      --connect jdbc:mysql://quickstart.cloudera:3306/retail_export \
      --username root \
      --password cloudera \
      --table result \
      --export-dir /user/cloudera/problem1/result4a-csv \
      --input-fields-terminated-by ',' \
      --input-lines-terminated-by '\n' \
      --outdir /home/cloudera/outdir \
      --bindir /home/cloudera/bindir

      SELECT * FROM result LIMIT 10;
     */
    sc.stop()
    spark.stop()
  }

}
