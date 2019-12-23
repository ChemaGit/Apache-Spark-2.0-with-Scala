package exercises_cert_4

import org.apache.spark.sql.SparkSession


/** Question 74
  * Problem Scenario 45 : You have been given 2 files , with the content as given Below
  * (technology.txt)
  * (salary.txt)
  * (technology.txt)
  * first,last,technology
  * Amit,Jain,java
  * Lokesh,kumar,unix
  * Mithun,kale,spark
  * Rajni,vekat,hadoop
  * Rahul,Yadav,scala
  * (salary.txt)
  * first,last,salary
  * Amit,Jain,100000
  * Lokesh,kumar,95000
  * Mithun,kale,150000
  * Rajni,vekat,154000
  * Rahul,Yadav,120000
  * Write a Spark program, which will join the data based on first and last name and save the joined results in following format, first Last.technology.salary
  *
  * Create the files and put them into HDFS
  * $ gedit /home/cloudera/files/technology.txt
  * $ gedit /home/cloudera/files/salary.txt
  * $ hdfs dfs -put /home/cloudera/files/technology.txt /user/cloudera/files/
  * $ hdfs dfs -put /home/cloudera/files/salary.txt /user/cloudera/files/
  */
object exercise_9 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 9")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  case class Technology(first: String, last: String, tech: String)
  case class Salary(firstN: String, lastN: String, salary: Int)

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val techDF = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/technology.txt")
        .map(line => line.split(","))
        .map(r => new Technology(r(0), r(1), r(2)))
        .toDF

    val salaryDF =  sc
      .textFile("hdfs://quickstart.cloudera/user/cloudera/files/salary.txt")
      .map(line => line.split(","))
      .map(r => new Salary(r(0), r(1), r(2).toInt))
      .toDF

    techDF.show()
    salaryDF.show()

    techDF.createOrReplaceTempView("technology")
    salaryDF.createOrReplaceTempView("salary")

    // TODO: finish it
    val result = spark.sqlContext.sql("""SELECT first, last, tech, salary FROM technology JOIN salary ON(first = firstN AND last = lastN) ORDER BY salary DESC""")
    result.show()

    result
      .rdd
      .map(r => "%s,%s,%s,%s".format(r(0).toString,r(1).toString,r(2).toString,r(3).toString))
      .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/exercises/question_74")


    sc.stop()
    spark.stop()
  }

}
