package spark_mlib.preparing_data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vectors

object IdentifyingOutliers {

  val spark = SparkSession
    .builder()
    .appName("IdentifyingOutliers")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","IdentifyingOutliers") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      /**
       * # MAHALANOBIS DISTANCE
       * 	- Multi-dimensional generalization of measuring how
       * many standard deviations a point is away from the mean
       * 	- Measured along each Principal Component axis
       * 	- Unitless and scale-invariant
       * 	- Takes into account the correlations of the dataset
       * 	- Used to detect outliers
       *
       * # EXAMPLE
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

      val dfOutlier = dfVec.select("id", "features")
        .unionAll(sqlContext.createDataFrame(Seq((10,Vectors.dense(3,3,3)))))

      dfOutlier.sort(dfOutlier("id").desc).show(5)

      /**
       * # AN EXAMPLE WITH OUTLIERS
       */
      val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeat")
        .setWithStd(true)
        .setWithMean(true)

      val scalerModel = scaler.fit(dfOutlier.select("id", "features"))
      val dfScaled = scalerModel.transform(dfOutlier).select("id", "scaledFeat")
      dfScaled.sort(dfScaled("id").desc).show(3)

      /**
       * # COMPUTING INVERSE OF COVARIANCE MATRIX
       */
      import breeze.linalg._
      import org.apache.spark.mllib.stat.Statistics
      val rddVec = dfScaled.select("scaledFeat").rdd.map(_(0).asInstanceOf[org.apache.spark.mllib.linalg.Vector])
      val colCov = Statistics.corr(rddVec)

      val invColCovB = inv(new DenseMatrix(3, 3, colCov.toArray))

      /**
       * # COMPUTING MAHALANOBIS DISTANCE
       */
      val mahalanobis = udf[Double, org.apache.spark.mllib.linalg.Vector] {
        v =>
          val vB = DenseVector(v.toArray)
          vB.t * invColCovB * vB
      }
      val dfMahalanobis = dfScaled.withColumn("mahalanobis", mahalanobis(dfScaled("scaledFeat")))
      dfMahalanobis.show(10, truncate = false)

      /**
       * # REMOVING OUTLIERS
       */

      dfMahalanobis.sort(dfMahalanobis("mahalanobis").desc).show(2)

      val ids = dfMahalanobis.select("id", "mahalanobis").sort(dfMahalanobis("mahalanobis").desc).drop("mahalanobis").collect()

      val idOutliers = ids.map(_(0).asInstanceOf[Long]).slice(0,2)
      println(idOutliers.mkString(","))

      dfOutlier.filter("id not in(10,2)").show(truncate = false)

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
