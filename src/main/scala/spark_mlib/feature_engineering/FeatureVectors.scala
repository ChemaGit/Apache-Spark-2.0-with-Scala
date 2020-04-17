package spark_mlib.feature_engineering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorSlicer

object FeatureVectors {
  val spark = SparkSession
    .builder()
    .appName("FeatureVectors")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","FeatureVectors") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  import spark.implicits._
  case class Customers(churn: Int, sessions: Int, revenue: Double, recency: Int)

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    try {
      /**
       * # MLLIB AND SPARK.ML
       *
       * - Spark's machine learning libraries are divided into two packages, MLlib and spark.ml
       * 	- MLlib is older, and is built on top of RDDs
       * 	- spark.ml is built on top of DataFrames, and can be used to construct ML pipelines
       * 	- In cases where MLlib and spark.ml offer equivalent functionality, this course will focus on spark.ml
       *
       * # FEATURE VECTORS FOR SUPERVISED LEARNING IN MLLIB AND SPARK.ML
       *
       * - MLlib
       * 	- The models in MLlib are designed to work with RDD[LabeledPoint] objects, which associates labels with feature vectors
       * - spark.ml
       * 	- The models in spark.ml are designed to work with DataFrames
       * 	- A basic spark.ml DataFrame will(bi default) have two columns:
       * 		- a label column (default name: "label")
       * 		- a features column (default name: "features")
       *
       * # CREATING FEATURE VECTORS I
       *
       * - The output of your ETL process might be a DataFrame with various columns.
       * For example, you might want to try to predict churn based on number of sessions, revenue, and recency:
       *
       * # CREATING FEATURE VECTORS II
       */


      val customers = sc.parallelize( List(Customers(1,20,61.24,103) ,
        Customers(1,8,80.64,23) ,
        Customers(0,4,100.94,42) ,
        Customers(0,8,99.48,26) ,
        Customers(1,17,120.56,47) )).toDF()

      /**
       * # CREATING FEATURE VECTORS III
       */
      val assembler = new VectorAssembler()
        .setInputCols(Array("sessions", "revenue", "recency"))
        .setOutputCol("features")
      val dfWithFeatures = assembler.transform(customers)
      dfWithFeatures.show(truncate = false)

      /**
       * # VECTORSLICERS
       */
      val slicer = new VectorSlicer()
        .setInputCol("features")
        .setOutputCol("some_features")

      slicer.setIndices(Array(0,1)).transform(dfWithFeatures).show(truncate = false)

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