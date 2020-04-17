package spark_mlib.preparing_data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.feature.{Tokenizer, RegexTokenizer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._

object TransformersAndStimators {

  val spark = SparkSession
    .builder()
    .appName("TransformersAndStimators")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","TransformersAndStimators") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      /**
       * # TRANSFORMERS
       * 	- Algorithm which can transform on DataFrame into another DataFrame
       * 	- Abstraction that includes feature transformers and learned models.
       * 	- Implements a method transform(), which converts one DataFrame into another,
       * generally by appending one or more columns
       * 	- Input and output columns set with setInputCol and setOutputCol methods
       * 	- Examples
       * 		- read one or more columns and map them into a new column of feature vectors
       * 		- read a column containing feature vectors and make a prediction for each vector
       *
       * # GENERAL PURPOSE TRANSFORMERS
       *
       * Transformer		Description					scikit-learn
       *
       * Binarizer		Threshold numerical feature to binary		Binarizer
       * Bucketizer		Bucket numerical features into ranges
       * ElementwiseProduct	Scale each feature/column separately
       * Normalizer		Scale each row to unit norm			Normalizer
       * OneHotEncoder		Encode k-category feature as binary features	OneHotEncoder
       * PolynomialExpansion	Create higher-order features			PolynomialFeatures
       * StandardScaler		Scale features to 0 mean and/or unit variance	StandardScaler
       * StringIndexer		Convert String feature to 0-based indices	LabelEncoder
       * VectorAssembler		Concatenate feature vectors			FeatureUnion
       * VectorIndexer		Identify categorical features, and index
       *
       * # TRANSFORMERS FOR NATURAL LAGUAGE PROCESSING
       *
       * Transformer		Description						scikit-learn
       *
       * HashingTF		Hash text/data to vector. Scale by term frequency	FeatureHasher
       * IDF			Scale features by inverse document frequency		TfidfTransformer
       * RegexTokenizer		Tokenize text using regular expressions			(part of text methods)
       * Tokenizer		Tokenize text on whitespace				(part of text methods)
       * Word2Vec		Learn vector representation of words
       *
       */

      /**
       * # TRANSFORMER EXAMPLE
       */
      import spark.implicits._

      val sentenceDataFrame = sqlContext
        .createDataFrame(Seq((0, "Hi I heard about Spark"),
          (1, "I wish Java could use case classes"),
          (2, "Logistic, regression, models, are, neat")))
        .toDF("label", "sentence")
      val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
      val tokenized = tokenizer.transform(sentenceDataFrame)
      tokenized.show(truncate = false)

      /**
       * # ESTIMATORS
       * 	- Algorithm which can be fit on a DataFrame to produce a Transformer
       * 	- Abstracts the concept of a learning algorithm or any algorithm that fits or trains on data
       * 	- Implements a method fit(), which accepts a DataFrame and produces a Model, which is a Transformer
       * 	- Example: LogisticRegression
       * 		- It is a learning algorithm and therefore an Estimator
       * 		- By calling the method fit() to train the logistic regression, a Model is returned
       */

      /**
       * # A SIMPLE EXAMPLE OF AN ESTIMATOR
       */
      val training = sqlContext.createDataFrame(Seq(
        (1.0, Vectors.dense(0.0, 1.1, 0.1)),
        (0.0, Vectors.dense(2.0, 1.0, -1.0)),
        (0.0, Vectors.dense(2.0, 1.3, 1.0)),
        (0.0, Vectors.dense(0.0, 1.2, -0.5)))).toDF("label", "features")
      val lr = new LogisticRegression()
      lr.setMaxIter(10).setRegParam(0.01)
      val model1 = lr.fit(training)
      model1.transform(training).show(truncate = false)

      /**
       * # PARAMETERS
       * 	- Transformers and Estimators use a uniform API for speciying parameters
       * 	- A ParamMap is a set of (parameter, value) pairs
       * 	- Parameters are specific to a given instance
       * 	- There are two main ways to pass parameters to an algorithm:
       * 		- Setting parameters for an instance using an appropiate method, for instance, setMaxIter(10)
       * 		- Passing a ParamMap to fit() or transform(), for instance, ParamMap(lr1.MaxIter -> 10, lr2.MaxIter -> 20)
       * In this case, the parameter MaxIter is being specified to two differen instances of models, lr1, lr2
       */

      /**
       * # A SIMPLE EXAMPLE OF PARAMETER SETTING
       */
      val paramMap = ParamMap(lr.maxIter -> 20, lr.regParam -> 0.01)
      val model2 = lr.fit(training, paramMap)
      model2.transform(training).show()

      /**
       * # VECTOR ASSEMBLER
       * 	- Transformer that combines a given list of columns into a single vector column
       * 	- Useful for combining raw features and features generated by other transformers into a single feature vector
       * 	- Accepts the following input column types:
       * 		- all numeric types
       * 		- boolean
       * 		- vector
       */

      /**
       * # AN EXAMPLE OF VectorAssembler
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
