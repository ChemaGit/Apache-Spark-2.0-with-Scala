package spark_mlib.preparing_data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Normalizer

import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler

object DataNormalization {

  val spark = SparkSession
    .builder()
    .appName("DataNormalization")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","DataNormalization") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      /**
       * # NORMALIZER
       * 	- A Transformer which transforms a dataset of Vector rows
       * , normalizing each Vector to have unit norm.
       * 	- Takes a parameter P, which specifies the p-norm
       * used for normalization (p=2 by default)
       * 	- Standardize input data and improve the behavior of learning algorithms
       */

      /**
       * EXAMPLE
       */
      val dfRandom = sqlContext.range(0,10).select("id")
        .withColumn("uniform", rand(10L))
        .withColumn("normal1", randn(10L))
        .withColumn("normal2", randn(11L))

      val assembler = new VectorAssembler().
        setInputCols(Array("uniform", "normal1", "normal2"))
        .setOutputCol("features")
      val dfVec = assembler.transform(dfRandom)
      dfVec.show(truncate = false)

      dfVec.select("id","features").show(truncate = false)

      val scaler1 = new Normalizer()
        .setInputCol("features")
        .setOutputCol("scaledFeat")
        .setP(1.0)
      scaler1.transform(dfVec.select("id", "features")).show(5, truncate = false)

      /**
       * # STANDARD SCALER
       * 	- A Model which can be fit on a dataset to produce a StandardScalerModel
       * 	- A Transformer which transforms a dataset of Vector rows, normalizing
       * each feature to have unit standard deviation and/or zero mean
       * 	- Takes two parameters:
       * 		- withStd: scales the data to unit standard deviation(default:true)
       * 		- withMean: centers the data with mean before scaling(default:false)
       * 	- It builds a dense output, sparse inputs will raise an exception
       * 	- If the standard deviation of a feature is zero, it returns 0.0 in the Vector for that feature
       */

      /**
       * # A SIMPLE STANDARD SCALER
       */
      val scaler2 = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeat")
        .setWithStd(true)
        .setWithMean(true)

      val scaler2Model = scaler2.fit(dfVec.select("id", "features"))
      scaler2Model.transform(dfVec.select("id","features")).show(5,truncate = false)

      /**
       * # MINMAX SCALER
       * 	- A model which can be fit on a dataset to produce a MinMaxScalerModel
       * 	- A Transformer which transforms a dataset of Vectors rows, rescaling each
       * feature to a specific range(often[0,1])
       * 	- Takes two parameters:
       * 		- min: lower bound after transformation, shared by all features (default: 0.0)
       * 		- max: upper bound after transformation, shared by all features (default: 1.0)
       * 	- Since zero values are likely to be transformed to non-zero values,
       * sparse inputs may result in dense outputs.
       */

      /**
       * # A SIMPLE MINMAX SCALER
       */
      val scaler3 = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeat")
        .setMin(-1.0)
        .setMax(1.0)

      val scaler3Model = scaler3.fit(dfVec.select("id", "features"))
      scaler3Model.transform(dfVec.select("id", "features")).show(5,truncate = false)

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
