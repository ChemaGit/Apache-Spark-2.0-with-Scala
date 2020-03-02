package exercises_cert_2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/** Question 49
  * Problem Scenario 88 : You have been given below three files
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
  * 1. It is possible that, same product can be supplied by multiple supplier. Now find each product, its price according to each supplier.
  * 2. Find all the supllier name, who are supplying 'Pencil 3B'
  * 3. Find all the products , which are supplied by ABC Traders.
  */

object exercise_10 {

  val warehouseLocation = "/home/hive/warehouse"

  val spark = SparkSession
    .builder()
    .appName("exercise_10")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_10")  // To silence Metrics warning
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir",warehouseLocation)
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val productSchema = StructType(List(StructField("pId", IntegerType, false), StructField("code",StringType, false),
        StructField("name", StringType, false), StructField("quantity",IntegerType, false),
        StructField("price",DoubleType, false), StructField("sId",IntegerType, false)))

      val supplierSchema = StructType(List(StructField("supId", IntegerType, false), StructField("name",StringType, false),
        StructField("phone", StringType, false)))

      val productSupplierSchema = StructType(List(StructField("prId", IntegerType, false), StructField("sppId",IntegerType, false)))

      val products = sqlContext
        .read
        .schema(productSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}product.csv")
        .cache()

      val supplier = sqlContext
        .read
        .schema(supplierSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}supplier.csv")
        .cache()

      val pr_sp = sqlContext
        .read
        .schema(productSupplierSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}products_suppliers.csv")
        .cache()

      products.createOrReplaceTempView("pr")
      supplier.createOrReplaceTempView("sp")
      pr_sp.createOrReplaceTempView("pr_sp")

      // 1. It is possible that, same product can be supplied by multiple supplier. Now find each product, its price according to each supplier.
      sqlContext
        .sql(
          """SELECT pId, code, pr.name AS ProductName, price, quantity, sp.name AS SupplierName
            |FROM pr JOIN pr_sp ON(pId = prId) JOIN sp ON(sppId = supId)""".stripMargin)
        .show()

      // 2. Find all the supllier name, who are supplying 'Pencil 3B'
      sqlContext
        .sql(
          """SELECT pr.name AS ProductName, sp.name AS SupplierName
            |FROM pr JOIN pr_sp ON(pId = prId) JOIN sp ON(sppId = supId)
            |WHERE pr.name LIKE("Pencil 3B")""".stripMargin)
        .show()

      // 3. Find all the products , which are supplied by ABC Traders.
      sqlContext
        .sql(
          """SELECT pr.name AS ProductName,sp.name AS SupplierName,code,quantity,price
            |FROM pr JOIN pr_sp ON(pId = prId) JOIN sp ON(sppId = supId)
            |WHERE sp.name LIKE("ABC Traders") """.stripMargin)
        .show()

      products.unpersist()
      supplier.unpersist()
      pr_sp.unpersist()

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped.")
      spark.stop()
      println("SparkSession stopped.")
    }
  }
}
