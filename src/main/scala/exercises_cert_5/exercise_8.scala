package exercises_cert_5

import org.apache.spark.sql.SparkSession

/** Question 91
  * Problem Scenario 82 : You have been given table in Hive(pruebas.t_product_parquet) with following structure (Which you have created in previous exercise).
*productid           	int
*productcode         	string
*name                	string
*quantity            	int
*price               	float
  *
  * productID,productCode,name,quantity,price
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
  *
  * productid int code string name string quantity int price float
  * Using SparkSQL accomplish following activities.
  * 1. Select all the products name and quantity having quantity <= 2000
  * 2. Select name and price of the product having code as 'PEN'
  * 3. Select all the products, which name starts with Pencil
  * 4. Select all products which "name" begins with 'P' followed by any two characters, followed by space, followed by zero or more characters
  *
  * $ gedit /home/cloudera/files/product.csv
  * $ hdfs dfs -put /home/cloudera/files/product.csv /user/cloudera/files/product.csv
  */

object exercise_8 {

  val spark = SparkSession
    .builder()
    .appName("exercise 8")
    .master("local[*]")
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

  case class Product(productID: Int,productCode: String,name: String,quantity: Int,price: Double)

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val productRdd = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/product.csv")
        .map(line => line.split(","))
        .filter(arr => !arr(0).equals("productID"))
        .map(arr => new Product(arr(0).toInt, arr(1),arr(2),arr(3).toInt,arr(4).toDouble))
        .toDF("productID","productCode","name","quantity","price")
        .persist()

    productRdd.show()

    productRdd.createOrReplaceTempView("products")

    // 1. Select all the products name and quantity having quantity <= 2000
    spark
        .sqlContext
        .sql("""SELECT name, quantity FROM products WHERE quantity <= 2000""")
        .show()

    // 2. Select name and price of the product having code as 'PEN'
    spark
        .sqlContext
        .sql("""SELECT name, price FROM products WHERE productCode = 'PEN' """)
        .show()

    // 3. Select all the products, which name starts with Pencil
    spark
        .sqlContext
        .sql("""SELECT * FROM products WHERE name LIKE("Pencil%") """)
        .show()

    // 4. Select all products which "name" begins with 'P' followed by any two characters, followed by space, followed by zero or more characters
    spark
        .sqlContext
        .sql("""SELECT * FROM products WHERE name LIKE("P__ %")""")
        .show()

    sc.stop()
    spark.stop()
  }
}