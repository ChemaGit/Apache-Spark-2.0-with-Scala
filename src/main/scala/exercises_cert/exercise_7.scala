package exercises_cert

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/** Question 9
  * Problem Scenario 89 : You have been given below patient data in csv format,
  * patientID,name,dateOfBirth,lastVisitDate
  * 1001,Ah Teck,1991-12-31,2012-01-20
  * 1002,Kumar,2011-10-29,2012-09-20
  * 1003,Ali,2011-01-30,2012-10-21
  * Accomplish following activities.
  * 1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
  * 2. Find all the patients who born in 2011
  * 3. Find all the patients age
  * 4. List patients whose last visited more than 60 days ago
  * 5. Select patients 18 years old or younger
  */

object exercise_7 {
  val spark = SparkSession
    .builder()
    .appName("exercise 7")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_7")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/patients.csv"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      val schema = StructType(List(StructField("id", IntegerType, false), StructField("name",StringType, false), StructField("birth", StringType, false), StructField("lastVisit",StringType, false)))

      val patients = sqlContext
        .read
        .schema(schema)
        .option("sep",",")
        .csv(path)
        .cache()

      patients.createOrReplaceTempView("patients")

      // 1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
      sqlContext.sql(
        """SELECT *
          |FROM patients
          |WHERE unix_timestamp(lastVisit, "yyyy-MM-dd") >= unix_timestamp("2012-09-15", "yyyy-MM-dd")""".stripMargin)
        .show()

      // 2. Find all the patients who born in 2011
      sqlContext.sql(
        """SELECT *
          |FROM patients
          |WHERE year(birth) = "2011" """.stripMargin)
        .show()

      // 3. Find all the patients age
      sqlContext.sql(
        """SELECT *, floor(datediff(current_date,birth)/ 365) AS age
          |FROM patients""".stripMargin)
        .show()

      // 4. List patients whose last visited more than 60 days ago
      sqlContext.sql(
        """SELECT *
          |FROM patients
          |WHERE datediff(current_date,lastVisit) > 60""".stripMargin).show()

      // 5. Select patients 18 years old or younger
      sqlContext.sql(
        """SELECT *,
          |floor(datediff(current_date,birth)/ 365) AS age
          |FROM patients
          |WHERE floor(datediff(current_date,birth)/ 365) <= 18""".stripMargin).show()

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

