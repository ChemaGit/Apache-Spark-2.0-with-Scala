package spark_mlib.feature_engineering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UsingExplodeUserDefinedFunctionsAndPivot {

  val spark = SparkSession
    .builder()
    .appName("UsingExplodeUserDefinedFunctionsAndPivot")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","UsingExplodeUserDefinedFunctionsAndPivot") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  import spark.implicits._
  import org.apache.spark.sql.functions._

  case class Sales(id: Int, account: String, year: String, commission: Int, sales_reps: Seq[String])

  /**
   * # User Defined Functions
   */
  val len: (Seq[String] => Int) = (arg: Seq[String]) => {arg.length}
  val column_len = udf(len)

  def main(args: Array[String]): Unit = {
    try {
      Logger.getRootLogger.setLevel(Level.ERROR)

      val sales = Seq(
        Sales(1, "Acme", "2013", 1000, Seq("Jim", "Tom")),
        Sales(2, "Lumos", "2013", 1100, Seq("Fred", "Ann")),
        Sales(3, "Acme", "2014", 2800, Seq("Jim")),
        Sales(4, "Lumos", "2014", 1200, Seq("Ann")),
        Sales(5, "Acme", "2014", 4200, Seq("Fred", "Sally"))
      ).toDF

      val exploded = sales
        .select(col("id"),col("account"), col("year"),
          (col("commission") / column_len(col("sales_reps"))).as("share"),
          explode(col("sales_reps")).as("sales_rep"))

      exploded.show(truncate = false)
      /**
       * # pivot()
       */
      exploded
        .groupBy(col("sales_rep"))
        .pivot("year")
        .agg(sum(col("share")))
        . orderBy("sales_rep")
        .show(truncate = false)


      // To have the opportunity to view the web console of Spark: http://localhost:4041/
      println("Type whatever to the console to exit......")
      scala.io.StdIn.readLine()
    } finally {
      sc.stop()
      println("SparkContext stopped")
      spark.stop()
      println("SparkSession stopped")
    }
  }
}