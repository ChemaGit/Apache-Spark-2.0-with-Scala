package exercises_cert_7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Question 6: Correct
  * PreRequiste:
*[Prerequisite section will not be there in actual exam]
*Run below sqoop command to import customer table from mysql into hdfs to the destination /user/cloudera/problem6/customer/text as text file and fields seperated by tab character
Only import customer_id,customer_fname,customer_city.
 **
 sqoop import \
--connect "jdbc:mysql://quickstart.cloudera/retail_db" \
--password cloudera \
--username root \
--table customers \
--fields-terminated-by '\t' \
--columns "customer_id,customer_fname,customer_city" \
--target-dir /user/cloudera/problem6/customer/ \
--outdir /home/cloudera/outdir \
--bindir /home/cloudera/bindir
 **
 Instructions:
*Find all customers that lives 'Brownsville' city and save the result into HDFS.
*Input folder is /user/cloudera/problem6/customer/text.
 **
 Output Requirement:
*Result should be saved in /user/cloudera/problem6/customer_Brownsville Output file should be saved in Json format
 **
 [You will not be provided with any answer choice in actual exam.Below answers are just provided to guide you]
*Important Information:
*Please make sure you are running all your solutions on spark 1.6 since it will be default spark version provided by exam environment.
*/

object exercise_3 {

  val spark = SparkSession
    .builder()
    .appName("exercise 1")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_3")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val schema = StructType(List(StructField("customer_id",IntegerType, false), StructField("customer_fname",StringType,false),StructField("customer_city",StringType,false)))

      val customers = sqlContext
          .read
          .option("sep","\t")
          .schema(schema)
          .csv("hdfs://quickstart.cloudera/user/cloudera/problem6/customer/")
          .cache

      customers.show(10)

      customers.createOrReplaceTempView("customers")

      val result = sqlContext
          .sql("""SELECT customer_id, customer_fname, customer_city FROM customers WHERE customer_city = "Brownsville" """)

      result
          .toJSON
          .write
          .json("hdfs://quickstart.cloudera/user/cloudera/problem6/customer_brownsville")

      // check the results
      // hdfs dfs -cat /user/cloudera/problem6/customer_brownsville/*.json | head -n 50

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
