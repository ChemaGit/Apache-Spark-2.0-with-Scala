package exercises_cert

import org.apache.spark.sql._

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
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 7").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val file = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/patients.csv")
      .map(lines => lines.split(","))
      .map(arr => (arr(0).toInt, arr(1), arr(2), arr(3)))
      .toDF("id","name","birth","lastVisit")
    file.show()

    file.registerTempTable("patients")

    val sqlContext = spark.sqlContext

    // 1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
    sqlContext.sql("""SELECT * FROM patients WHERE unix_timestamp(lastVisit, "yyyy-MM-dd") >= unix_timestamp("2012-09-15", "yyyy-MM-dd")""").show()

    // 2. Find all the patients who born in 2011
    sqlContext.sql("""SELECT * FROM patients WHERE year(birth) = "2011" """).show()

    // 3. Find all the patients age
    sqlContext.sql("""SELECT *, floor(datediff(current_date,birth)/ 365) AS age FROM patients""").show()

    // 4. List patients whose last visited more than 60 days ago
    sqlContext.sql("""SELECT * FROM patients WHERE datediff(current_date,lastVisit) > 60""").show()

    // 5. Select patients 18 years old or younger
    sqlContext.sql("""SELECT *, floor(datediff(current_date,birth)/ 365) AS age FROM patients WHERE floor(datediff(current_date,birth)/ 365) <= 18""").show()

    sc.stop()
    spark.stop()
  }
}

