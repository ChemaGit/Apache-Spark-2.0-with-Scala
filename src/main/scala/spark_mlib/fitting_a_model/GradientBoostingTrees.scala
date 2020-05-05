package spark_mlib.fitting_a_model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.GBTClassificationModel

import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.ml.regression.GBTRegressionModel

object GradientBoostingTrees {
  val spark = SparkSession
    .builder()
    .appName("DecisionTress")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","DecisionTress") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      /**
       * ### GRADIENT-BOOSTING TREES
       * 	- Like Random forests, they are ensembles of Decision Trees
       * 	- Iteratively train decision trees in order to minimize a loss function
       * 	- Supports binary classification
       * 	- Supports regression
       * 	- Supports continuous and categorical features
       *
       * # BASIC ALGORITHM
       * 	- Iteratively trains a sequence of decision trees
       * 	- On each iteration, uses the current ensemble to make label predictions
       * and compares it to true labels
       * 	- Re-labels dataset to put more emphasis on instances with poor
       * predictions, according to a loss function
       * 	- With each iteration, reduces the loss function, thus correct for previous mistakes
       * 	- Supported loss functions:
       * 		- classification: Log Loss(twice binomial negative log likelihood)
       * 		- regression: Squared Error(L2 loss, default) and Absolute Error(L1 loss, more robust to outliers)
       *
       * # GRADIENT-BOOSTED TREES PARAMETERS
       * 	- loss: loss function(Log Loss, for classification, Squared and Absolute errors, for regression)
       * 	- numIterations: number of trees in the ensemble
       * 		- each iteration produces one tree
       * 		- if it increases:
       * 			- model gets more expressive, improving training data accuracy
       * 			- test-time accuracy may suffer(if too large)
       * 	- learningRate: should NOT need to be tuned
       * 		- if behavior seems unstable, decreasing it may improve stability
       *
       * # VALIDATION WHILE TRAINING
       * 	- Gradient-Boosted Trees can overfit when trained with more trees
       * 	- The method runWithValidation allows validation while training
       * 		- takes a pair of RDDs: training and validation datasets
       * 	- Training is stopped when validation error improvement is less than tolerance
       * specified as validationTol in BoostingStrategy
       * 		- validation error decreases initially and later increases
       * 		- there might be cases in which the validation error does not change monotolically
       * 			- set a large enough negative tolerance
       * 			- examine validation curve using evaluateEachIteration, which gives the error or loss per iteration
       * 			- tune the number of iterations
       *
       * # INPUTS AND OUTPUTS
       *
       * Param name		Type(s)		Default		Description
       *
       * LabelCol		Double		"label"		Label to predict
       * featuresCol		Vector		"features"	Feature vector
       *
       * Param name		Type(s)		Default		Description	Notes
       *
       * predictionCol		Double		"prediction"	Predicted label
       */

      /**
       * # LOADING DATA
       */
      import sqlContext.implicits._

      // get the data into /tmp
      // wget -P /tmp https://raw.githubusercontent.com/apache/spark/master/data/mllib/sample_libsvm_data.txt

      val data = MLUtils.loadLibSVMFile(sc, "/tmp/sample_libsvm_data.txt")
        .map(f => (f.label, f.features.asML))
        .toDF("label", "features")

      data.show(5,truncate = false)

      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

      val labelIndexer = new StringIndexer().setInputCol("label")
        .setOutputCol("indexedLabel").fit(data)

      val labelConverter = new IndexToString().setInputCol("prediction")
        .setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

      val featureIndexer = new VectorIndexer().setInputCol("features")
        .setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

      /**
       * # GBT CLASSIFICATION II
       */
      val gbtC = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10)
      val pipelineGBTC = new Pipeline().setStages(Array(labelIndexer,featureIndexer,gbtC, labelConverter))
      val modelGBTC = pipelineGBTC.fit(trainingData)

      val predictionsGBTC = modelGBTC.transform(testData)
      predictionsGBTC.select("predictedLabel", "label", "features").show(3, truncate = false)

      val gbtModelC = modelGBTC.stages(2).asInstanceOf[GBTClassificationModel]
      println("Learned classification GBT model:\n" + gbtModelC.toDebugString)

      /**
       * # RBT REGRESSION
       */
      val gbtR = new GBTRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures").setMaxIter(10)
      val pipelineGBTR = new Pipeline().setStages(Array(featureIndexer, gbtR))

      val modelGBTR = pipelineGBTR.fit(trainingData)

      val predictionsGBTR = modelGBTR.transform(testData)

      predictionsGBTR.show(5, truncate = false)

      /**
       * # RANDOM FORESTS VS GBTs
       * 	- Number of trees
       * 		- RFs: more trees reduce variance and the likelihood of averfitting;
       * improves performance monotonically
       * 		- GBTs: more trees reduce bias, but increase the likelihood of overfitting and
       * performance can start to decrease if the number of trees grows too large
       * 	- Parallelization
       * 		- RFs: can train multiples trees in parallel
       * 		- GBTs: train one tree at a time
       * 	- Depth of trees
       * 		- RFs: deeper trees
       * 		- GBTs: shallower trees
       */

      // To have the opportunity to view the web console of Spark: http://localhost:4040/
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
