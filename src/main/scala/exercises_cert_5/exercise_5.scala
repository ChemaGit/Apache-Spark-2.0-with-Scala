package exercises_cert_5

import org.apache.spark.sql.SparkSession

/** Question 84
  * Problem Scenario 33 : You have given a files as below.
  * spark5/EmployeeName.csv (id,name)
  * spark5/EmployeeSalary.csv (id,salary)
  * Data is given below:
  * EmployeeName.csv
  * E01,Lokesh
  * E02,Bhupesh
  * E03,Amit
  * E04,Ratan
  * E05,Dinesh
  * E06,Pavan
  * E07,Tejas
  * E08,Sheela
  * E09,Kumar
  * E10,Venkat
  * EmployeeSalary.csv
  * E01,50000
  * E02,50000
  * E03,45000
  * E04,45000
  * E05,50000
  * E06,45000
  * E07,50000
  * E08,10000
  * E09,10000
  * E10,10000
  * Now write a Spark code in scala which will load these two files from hdfs and join the same, and produce the (name, salary) values.
  * And save the data in multiple file group by salary (Means each file will have name of employees with same salary). Make sure file name include salary as well.
  *
  * $ gedit /home/cloudera/files/EmployeeName.csv &
  * $ gedit /home/cloudera/files/EmployeeSalary.csv &
  * $ hdfs dfs -put /home/cloudera/files/EmployeeName.csv  /user/cloudera/files
  * $ hdfs dfs -put /home/cloudera/files/EmployeeSalary.csv /user/cloudera/files
  */


object exercise_5 {

  lazy val spark = SparkSession
    .builder()
    .appName("exercise 5")
    .master("local[*]")
    .getOrCreate()

  lazy val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")

    val emp = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/EmployeeName.csv")
        .map(line => line.split(","))
        .map(arr => (arr(0),arr(1)))

    val salary = sc
        .textFile("hdfs://quickstart.cloudera/user/cloudera/files/EmployeeSalary.csv")
        .map(line => line.split(","))
        .map(arr => (arr(0), arr(1)))

    val joined = salary
        .join(emp)
        .map({case( (id,(sal, name)) ) => (sal, name)})
        .groupByKey()

    joined.foreach({
      case(s, n) => sc
      .makeRDD(n.toList)
      .saveAsTextFile(s"hdfs://quickstart.cloudera/user/cloudera/exercises/question_84/salary_$s")
    })

    sc.stop()
    spark.stop()
  }

}
