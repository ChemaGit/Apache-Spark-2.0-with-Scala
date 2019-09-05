package exercises_cert_1

/** Question 35
  * Problem Scenario 67 : You have been given below code snippet.
  * lines = sc.parallelize(['its fun to have fun,','but you have to know how.'])
  * r1 = lines.map(lambda x: x.replace(',','').replace('.','').lower())
  * r2 = r1.flatMap(lambda x: x.split(' '))
  * r3 = r2.map(lambda x: (x, 1))
  * operation1
  * r5 = r4.map(lambda x:(x[1],x[0]))
  * r6 = r5.sortByKey(ascending=False)
  * r6.take(20)
  * Write a correct code snippet for operationl which will produce desired output, shown below.
  * [(2, 'fun'), (2, 'to'), (2, 'have'), (1, 'its'), (1, 'know'), (1, 'how'), (1, 'you'), (1, 'but')]
  */

import org.apache.spark.sql._

object exercise_10 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("exercise 10").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    sc.stop()
    spark.stop()
  }
}
