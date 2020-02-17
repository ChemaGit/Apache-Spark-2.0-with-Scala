package exercises_cert_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * Question 5: Correct
  * PreRequiste:
*[Prerequisite section will not be there in actual exam]
*Run below sqoop command to import customer table from mysql into hdfs to the destination /user/cloudera/problem5/customer/parquet as parquet file.
*Only import customer_id,customer_fname,customer_city.
 **
 sqoop import \
*--connect jdbc:mysql://localhost/retail_db \
*--password cloudera \
*--username root \
*--table customers \
*--columns "customer_id,customer_fname,customer_city" \
*--target-dir /user/cloudera/problem5/customer/parquet \
*--as-parquetfile \
*--outdir /home/cloudera/outdir \
*--bindir /home/cloudera/bindir
 **
 Instructions:
 **
 Count number of customers grouped by customer city and customer first name where customer_fname is like "Mary" and order the results by customer first name and save the result as text file.
*Input folder is /user/cloudera/problem5/customer/parquet.
 **
 Output Requirement:
*Result should have customer_city,customer_fname and count of customers and output should be saved in /user/cloudera/problem5/customer/output as text file with fields separated by pipe character
*/

object exercise_2 {

  val spark = SparkSession
    .builder()
    .appName("exercise 1")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_10")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  case class Customers(customer_id: Int,customer_fname: String, customer_city: String)

  val rootPath = "hdfs://quickstart.cloudera/user/cloudera/problem5/customer/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val customers = sqlContext
          .read
          .parquet(s"${rootPath}parquet")
          .cache
      customers.show(10)

      // Count number of customers grouped by customer city and customer first name where customer_fname is like "Mary" and order the results by customer first name
      // and save the result as text file with fields separated by pipe character
      customers.createOrReplaceTempView("customers")

      val output = sqlContext
          .sql(
            """SELECT customer_fname, customer_city, COUNT(customer_id) AS count_customers
              |FROM customers
              |WHERE customer_fname LIKE("%Mary%")
              |GROUP BY customer_city, customer_fname
              |ORDER BY customer_fname
            """.stripMargin)

      output.show(10)

      output
          .write
          .option("header",true)
          .option("sep","|")
          .csv(s"${rootPath}output")

      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop
      println("SparkContext stopped.")
      spark.stop
      println("SparkSession stopped.")
    }

  }

}
