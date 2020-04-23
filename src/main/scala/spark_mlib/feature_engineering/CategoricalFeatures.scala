package spark_mlib.feature_engineering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.linalg._

object CategoricalFeatures {

  val spark = SparkSession
    .builder()
    .appName("CategoricalFeatures")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","CategoricalFeatures") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      /**
       * # MOTIVATION
       * 	- Categorical variables can take on only a limited number of
       * possible values, like country, or gender
       * 	- They represent reality. You don't have infinite variation in between two countries.
       * You do have infinite values between two integers
       * 	- Categories are less useful than integers for computations.
       * So internally a computer will "translate" categorical variables to integers.
       * 	- In R you have factors
       * 	- In Python pandas you have the categorical data type.
       * What is the equivalent data structure in Spark?
       * 	- These structures usually map strings to integers in a way that makes future
       * computations easier. In this video will see how Spark does it.
       *
       * # Why Are Integers Better?
       * 	- Spark's classifiers and regressors only work with numerical features;
       * string features must be converted to numbers a StringIndexer
       * 	- This helps keep Spark's internals simpler and more efficient
       * 	- There's little cost in transforming categorical features to numbers,
       * and then back to strings
       */

      /**
       * # Using a StringIndexer
       */
      import spark.implicits._
      import org.apache.spark.sql.functions._

      val df = sqlContext.createDataFrame(
        Seq((0,"US"),(1,"UK"),(2,"FR"),(3,"US"),(4,"US"),(5,"FR"))
      ).toDF("id", "nationality")

      df.show(truncate = false)

      /**
       * # Understanding the Output of a StringIndexer
       */
      val indexer = new StringIndexer().
        setInputCol("nationality").
        setOutputCol("nIndex")

      val indexed = indexer.fit(df).transform(df)
      indexed.show(truncate = false)

      /**
       * # Reversing the Mapping
       * 	- The classifiers in MLlib and spark.ml will predict
       * numeric values that correspond to the index values
       * 	- IndexToString is what you'll need to transform these
       * numbers back into your original labels.
       */

      /**
       * # IndexToString Example
       */
      val converter = new IndexToString().
        setInputCol("predictedIndex")
        . setOutputCol("predictedNationality")

      val predictions = indexed
        .selectExpr(
          "nIndex as predictedIndex")
      converter.transform(predictions).show(truncate = false)

      /**
       * # One Hot Encoding
       * 	- Suppose we are trying to fit a linear regressor that uses nationality as a feature
       * 	- It would be impossible to learn a weight for this one feature that can distinguish
       * between the 3 nationalities in our dataset
       * 	- It's better to instead have a separate Boolean feature for each nationality, and
       * learn weights for those features independently.
       *
       *
       * # Spark's OneHotEncoder
       * 	- The OneHotEncoder creates a sparse vector column,
       * with each dimension of this vector of Booleans
       * representing one of the possible values of the original feature.
       */

      /**
       * # Using a OneHotEncoder
       */
      val encoder = new OneHotEncoder()
        .setInputCol("nIndex").setOutputCol("nVector")
      val encoded = encoder.transform(indexed)



      encoded.foreach { c =>
        val dv = c.getAs[SparseVector]("nVector").toDense
        println(s"${c(0)} ${c(1)} $dv")
      }

      /**
       * # The dropLast Option
       */
      val encoder1 = new OneHotEncoder()
        .setInputCol("nIndex").setOutputCol("nVector").setDropLast(false)
      val encoded1 = encoder1.transform(indexed)
      val toDense = udf[DenseVector, SparseVector](_.toDense)
      encoded1.withColumn("denseVector", toDense(encoded1("nVector"))).show(truncate = false)

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
