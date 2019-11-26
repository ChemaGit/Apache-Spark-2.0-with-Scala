package exercises_cert_3

import org.apache.spark.sql.SparkSession

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
object exercise_6 {

  /**
    * create the files and put then in HDFS
    * $ gedit /home/cloudera/files/fee.txt &
    * $ gedit /home/cloudera/files/course.txt &
    * $ hdfs dfs -put /home/cloudera/files/course.txt /user/cloudera/files/
    * $ hdfs dfs -put /home/cloudra/files/fee.txt /user/cloudera/files/
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("exercise 6")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val course = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/course.txt")
        .map(line => line.split(","))
        .map(r => (r(0).toInt, r(1)))
        .toDF("idC", "course")

    val fee = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/files/fee.txt")
        .map(line => line.split(","))
        .map(r => (r(0).toInt, r(1).toInt))
        .toDF("idF", "fee")

    course.createOrReplaceTempView("course")
    fee.createOrReplaceTempView("fee")

    // 1. Select all the courses and their fees , whether fee is listed or not.
    spark.sqlContext.sql("""SELECT course, fee FROM course LEFT OUTER JOIN fee ON(idC = idF)""").show()
    // 2. Select all the available fees and respective course. If course does not exists still list the fee
    spark.sqlContext.sql("""SELECT course, fee FROM course RIGHT OUTER JOIN fee ON(idC = idF)""").show()
    // 3. Select all the courses and their fees , whether fee is listed or not. However, ignore records having fee as null.
    spark.sqlContext.sql("""SELECT course, fee FROM course LEFT OUTER JOIN fee ON(idC = idF) WHERE fee IS NOT NULL""").show()
    spark.sqlContext.sql("""SELECT course, fee FROM course JOIN fee ON(idC = idF)""").show()

    sc.stop()
    spark.stop()
  }
}
