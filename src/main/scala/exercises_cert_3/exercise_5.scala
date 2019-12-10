package exercises_cert_3

import org.apache.spark.sql.SparkSession

/** Question 54
  * Problem Scenario 87 : You have been given below three files
  * product.csv (Create this file in hdfs)
  * productID,productCode,name,quantity,price,supplierid
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
  * supplier.csv
  * supplierid,name,phone
  * 501,ABC Traders,88881111
  * 502,XYZ Company,88882222
  * 503,QQ Corp,88883333
  * products_suppliers.csv
  * productID,supplierID
  * 2001,501
  * 2002,501
  * 2003,501
  * 2004,502
  * 2001,503
  * 1001,501
  * 1002,501
  * 1003,501
  * 1004,502
  * 1005,502
  * 1006,502
  * Now accomplish all the queries given in solution.
  * Select product, its price , its supplier name where product price is less than 0.6 using SparkSQL
  */

object exercise_5 {
  /**
    * Building files
    * $ gedit /home/cloudera/files/product.csv &
    * $ gedit /home/cloudera/files/supplier.csv &
    * $ gedit /home/cloudera/files/products_suppliers.csv &
    * $ hdfs dfs -put /home/cloudera/files/product.csv /user/cloudera/files
    * $ hdfs dfs -put /home/cloudera/files/supplier.csv /user/cloudera/files
    * $ hdfs dfs -put /home/cloudera7files/products_suppliers.csv /user/cloudera/files
    */

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 5")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val product = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/product.csv")
        .map(line => line.split(","))
        .filter(r => !r(0).contains("productID"))
        .map(r => (r(0).toInt, r(1), r(2), r(3).toInt, r(4).toDouble))
        .toDF("pId","code","name","quantity","price")

    val supplier = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/supplier.csv")
        .map(line => line.split(","))
        .map(r => (r(0).toInt, r(1), r(2)))
        .toDF("sId","sName", "phone")

    val pr_sp = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/products_suppliers.csv")
        .map(line => line.split(","))
        .map(r => (r(0).toInt, r(1).toInt))
        .toDF("prId", "spId")

    product.createOrReplaceTempView("pr")
    supplier.createOrReplaceTempView("sp")
    pr_sp.createOrReplaceTempView("pr_sp")

    spark.sqlContext.sql("""SELECT code, name, price, sName, phone FROM pr JOIN pr_sp ON(pId = prId) JOIN sp ON(spId = sId) WHERE price < 0.6""").show()

    sc.stop()
    spark.stop()
  }

}