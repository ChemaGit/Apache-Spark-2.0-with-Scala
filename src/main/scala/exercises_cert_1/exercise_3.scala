package exercises_cert_1

/** Question 20
  * Problem Scenario 83 : In Continuation of previous question, please accomplish following activities.
  * 1. Select all the records with quantity >= 5000 and name starts with 'Pen'
  * 2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
  * 3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen'
  * 4. Select all the products which name is 'Pen Red', 'Pen Black'
  * 5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.
  *
  * product.csv
  * productID,productCode,name,quantity,price,supplierID
  * 1001,PEN,Pen Red,5000,1.23,501
  * 1002,PEN,Pen Blue,8000,1.25,501
  * 1003,PEN,Pen Black,2000,1.25,501
  * 1004,PEC,Pencil 2B,10000,0.48,502
  * 1005,PEC,Pencil 2H,8000,0.49,502
  * 1006,PEC,Pencil HB,0,9999.99,502
  * 2001,PEC,Pencil 3B,500,0.52,501
  * 2002,PEC,Pencil 4B,200,0.62,501
  * 2003,PEC,Pencil 5B,100,0.73,501
  * 2004,PEC,Pencil 6B,500,0.47,502
  */

// Previous steps
// $ hdfs dfs -put /home/cloudera/files/product.csv /user/cloudera/files/

import org.apache.spark.sql._

object exercise_3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 3").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val products = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/product.csv")
      .map(line => line.split(","))
      .filter(arr => arr(0) != "productID")
      .map(arr => (arr(0).toInt,arr(1),arr(2),arr(3).toInt,arr(4).toFloat)).toDF("id","code","name","quantity","price")

    products.registerTempTable("products")
    spark.sql("""SELECT * FROM products""")
    // 1. Select all the records with quantity >= 5000 and name starts with 'Pen'
    spark.sql("""SELECT * FROM products WHERE quantity >= 5000 and name LIKE('Pen%')""").show()
    // 2. Select all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
    spark.sql("""SELECT * FROM products WHERE quantity >= 5000 and price < 1.24 and name LIKE('Pen%')""").show()
    // 3. Select all the records witch does not have quantity >= 5000 and name does not starts with 'Pen'
    spark.sql("""SELECT * FROM products WHERE NOT (quantity >= 5000 and name LIKE('Pen%'))""").show()
    // 4. Select all the products which name is 'Pen Red', 'Pen Black'
    spark.sql("""SELECT * FROM products WHERE name IN("Pen Red","Pen Black")""").show()
    // 5. Select all the products which has price BETWEEN 1.0 AND 2.0 AND quantity BETWEEN 1000 AND 2000.
    spark.sql("""SELECT * FROM products WHERE price BETWEEN 1.0 and 2.0 AND quantity BETWEEN 1000 AND 2000""").show()

    sc.stop()
    spark.stop()
  }
}

