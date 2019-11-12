package exercises_cert_2

import org.apache.spark.sql.SparkSession

/** Question 48
  * Problem Scenario 76 : You have been given MySQL DB with following details.
  * user=retail_dba
  * password=cloudera
  * database=retail_db
  * table=retail_db.orders
  * table=retail_db.order_items
  * jdbc URL = jdbc:mysql://quickstart:3306/retail_db
  * Columns of order table : (orderid , order_date , ordercustomerid, order_status)
  * Columns of order_items table : (order_item_id , order_item_order_id ,order_item_product_id, order_item_quantity,order_item_subtotal,order_item_product_price)
  * Please accomplish following activities.
  * 1. Copy "retail_db.orders" table to hdfs in a directory question48/orders
  * 2. Once data is copied to hdfs, using spark-shell calculate the number of order for each status.
  * 3. Use all the following methods to calculate the number of order for each status. (You need to know all these functions and its behavior for real exam)
  * -countByKey()
  * -groupByKey()
  * -reduceByKey()
  * -aggregateByKey()
  * -combineByKey()
  */

/*
sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--as-textfile \
--delete-target-dir \
--target-dir /user/cloudera/exercise_9/orders \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/exercise_9/orders
$ hdfs dfs -text /user/cloudera/exercise_9/orders/part-m* | head -n 20

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table order_items \
--delete-target-dir \
--target-dir /user/cloudera/exercise_9/order_items \
--as-textfile \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir \
--num-mappers 1

$ hdfs dfs -ls /user/cloudera/exercise_9/order_items
$ hdfs dfs -text /user/cloudera/exercise_9/order_items/part-m* | head -n 20
 */

object exercise_9 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 9").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val orders = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/exercise_9/orders")
        .map(line => line.split(","))
        .map(arr => (arr(3),1))

    // count by key
    val countByKey = orders.countByKey()
    countByKey.foreach(println)
    sc.parallelize(countByKey.toList)
      .sortBy(t => t._2, false)
      .repartition(1)
      .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercise_9/count_by_key")
    println("*********************")

    // group by key
    val groupByKey = orders.groupByKey()
        .map(t => (t._1,t._2.sum))
        .sortBy(t => t._2, false)
    groupByKey
      .collect
      .foreach(println)
    groupByKey
        .repartition(1)
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercise_9/group_by_key")
    println("*********************")

    // reduce by key
    val reduceByKey = orders
      .reduceByKey( (v,c) => v + c)
        .sortBy(t => t._2, false)
    reduceByKey
      .collect
      .foreach(println)
    reduceByKey
        .repartition(1)
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercise_9/reduce_by_key")
    println("*********************")

    // aggregate by key
    val aggregateByKey = orders.aggregateByKey(0)(((z: Int,v: Int) => z + v), ((v: Int, c: Int) => v + c))
        .sortBy(t => t._2, false)
    aggregateByKey.collect
        .foreach(println)
    aggregateByKey
        .repartition(1)
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercise_9/aggregate_by_key")

    println("*********************")
    // combine by key
    val combineByKey = orders.combineByKey(((v: Int) => v), ((v: Int, c: Int) => v + c), ((v: Int, c: Int) => v + c))
        .sortBy(t => t._2, false)
    combineByKey
      .collect
        .foreach(println)
    combineByKey
        .repartition(1)
        .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercise_9/combine_by_key")


    sc.stop()
    spark.stop()
  }

}
