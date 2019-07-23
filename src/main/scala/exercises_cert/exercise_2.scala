/** Question 2
  * Problem Scenario 81 : You have been given MySQL DB with following details. You have
  * been given following product.csv file
  * product.csv
  * productID,productCode,name,quantity,price
  * 1001,PEN,Pen Red,5000,1.23
  * 1002,PEN,Pen Blue,8000,1.25
  * 1003,PEN,Pen Black,2000,1.25
  * 1004,PEC,Pencil 2B,10000,0.48
  * 1005,PEC,Pencil 2H,8000,0.49
  * 1006,PEC,Pencil HB,0,9999.99
  * Now accomplish following activities.
  * 1. Create a Hive ORC table using SparkSql
  * 2. Load this data in Hive table.
  * 3. Create a Hive parquet table using SparkSQL and load data in it.
  */
//Move the file from local to HDFS
$ hdfs dfs -put /home/cloudera/files/product.csv /user/cloudera/files_backup

val product = sc.textFile("/user/cloudera/files_backup/product.csv")
val rdd = product.map(line => line.split(",")).filter(arr => arr(0) != "productID").map(arr => (arr(0).toInt,arr(1),arr(2),arr(3).toInt,arr(4).toDouble)).toDF("id","code","name","quantity","price")
rdd.write.orc("/user/cloudera/exercise_2/orc")
spark.sqlContext.sql("use default")
spark.sqlContext.sql("""CREATE TABLE product_orc(id int, code string, name string, quantity int, price double) STORED AS ORC LOCATION "/user/cloudera/exercise_2/orc" """)
spark.sqlContext.sql("""SELECT * FROM product_orc""").show()

rdd.write.parquet("/user/cloudera/exercise_2/parquet")
spark.sqlContext.sql("""CREATE TABLE product_parquet(id int, code string, name string, quantity int, price double) STORED AS PARQUET LOCATION "/user/cloudera/exercise_2/parquet" """)
spark.sqlContext.sql("""SELECT * FROM product_parquet""").show()