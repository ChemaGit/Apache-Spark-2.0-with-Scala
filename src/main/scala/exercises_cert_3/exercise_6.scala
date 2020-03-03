package exercises_cert_3

/** Question 55
  * Problem Scenario 90 : You have been given below two files
  * course.txt
  * id,course
  * 1,Hadoop
  * 2,Spark
  * 3,HBase
  * fee.txt
  * id,fee
  * 2,3900
  * 3,4200
  * 4,2900
  * Accomplish the following activities.
  * 1. Select all the courses and their fees , whether fee is listed or not.
  * 2. Select all the available fees and respective course. If course does not exists still list the fee
  * 3. Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object exercise_6 {

  /**
    * create the files and put then in HDFS
    * $ gedit /home/cloudera/files/fee.txt &
    * $ gedit /home/cloudera/files/course.txt &
    * $ hdfs dfs -put /home/cloudera/files/course.txt /user/cloudera/files/
    * $ hdfs dfs -put /home/cloudra/files/fee.txt /user/cloudera/files/
    */

  val spark = SparkSession
    .builder()
    .appName("exercise_6")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "exercise_6")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  val path = "hdfs://quickstart.cloudera/user/cloudera/files/"

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      val courseSchema = StructType(List(StructField("idC", IntegerType, false), StructField("course",StringType, false)))

      val feeSchema = StructType(List(StructField("idF", IntegerType, false), StructField("fee",IntegerType, false)))

      val course = sqlContext
        .read
        .schema(courseSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}course.txt")
        .cache()

      val fee = sqlContext
        .read
        .schema(feeSchema)
        .option("header", false)
        .option("sep",",")
        .csv(s"${path}fee.txt")
        .cache()

      course.createOrReplaceTempView("course")
      fee.createOrReplaceTempView("fee")

      sqlContext
          .sql(
            """SELECT *
              |FROM course JOIN fee ON(idC = idF)""".stripMargin)
          .show()

      // 1. Select all the courses and their fees , whether fee is listed or not.
      sqlContext.sql(
        """SELECT course, fee
          |FROM course LEFT OUTER JOIN fee ON(idC = idF)""".stripMargin)
        .show()

      // 2. Select all the available fees and respective course. If course does not exists still list the fee
      sqlContext.sql(
        """SELECT course, fee
          |FROM course RIGHT OUTER JOIN fee ON(idC = idF)""".stripMargin)
        .show()

      // 3. Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null.
      sqlContext.sql(
        """SELECT course, fee
          |FROM course LEFT OUTER JOIN fee ON(idC = idF)
          |WHERE fee IS NOT NULL""".stripMargin)
        .show()

      sqlContext.sql(
        """SELECT course, fee
          |FROM course JOIN fee ON(idC = idF)""".stripMargin)
        .show()

      course.unpersist()
      fee.unpersist()

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
