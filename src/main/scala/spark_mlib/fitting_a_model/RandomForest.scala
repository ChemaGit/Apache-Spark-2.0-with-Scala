package spark_mlib.fitting_a_model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel

import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.regression.RandomForestRegressionModel


object RandomForest {
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
       * # ENSEMBLE METHOD
       * 	- Learning algorithm which creates a model
       * composed of a set of other base models
       * 	- 'Random Forest' and 'Gradient-Boosted Trees' are
       * ensemble algorithms based on decision trees
       * 	- Among top performers for classification and regression problems
       *
       * # RANDOM FOREST(RFs)
       * 	- Ensembles of Decision Trees
       * 	- One of the most successful machine learning models  for classification and regression.
       * 	- Combine many decision trees in order to reduce the risk of averfitting
       * 	- Supports binary and multiclass classification
       * 	- Supports regression
       * 	- Supports continuous and categorical features
       *
       * # RF: BASIC ALGORITHM
       * 	- RF trains a bunch of decision trees separately
       * 	- RF Injects randomness into the training process
       * 		- bootstrapping: subsamples the original dataset on each
       * iteration to get a different training set
       * 		- considers different random subsets of features to split on at each tree node
       * 	- Combined predictions from several trees reduces the variance of the
       * predictions and improves the performance on test data
       * 		- classification: majority vote - each tree's prediction is counted as a vote for
       * one class and the predicted label is the class with largest number of votes
       * 		- regression: average-each tree predicts a real value and the predicted label is
       * equal to the average of all predictions
       *
       * # RANDOM FOREST PARAMETERS I
       * 	- Most important parameters in Spark.ml implementation
       * 	- Parameters that CAN be tuned to improve performance:
       * 		- numTrees: number of trees in the forest. If it increases:
       * 			- the variance of predictions decreases, improving test-time accuracy
       * 			- training time increases roughly linearly
       * 		- maxDepth: maximum depth of each tree in the forest. If it increases:
       * 			- model gets more expressive and powerful
       * 			- takes longer to train
       * 			- is more prone to overfitting
       *
       * # RANDOM FOREST PARAMETERS II
       * 	- subsamplingRate: specifies the fraction of size of the original dataset to be used for
       * training each tree in the forest
       * 		- default = 1.0, but decreasing it can speed up training
       * 	- featureSubsetStrategy: specifies the fraction of total number of features to use as
       * candidates for splitting at each tree node
       * 		- decreasing it will speed up training
       * 		- if too low, can also impact performance
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

      /**
       * # RF CLASSIFICATION
       */

      val labelIndexer = new StringIndexer().setInputCol("label")
        .setOutputCol("indexedLabel").fit(data)

      val labelConverter = new IndexToString().setInputCol("prediction")
        .setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

      val featureIndexer = new VectorIndexer().setInputCol("features")
        .setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

      val rfC = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(3)

      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

      val pipelineRFC = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rfC, labelConverter))
      val modelRFC = pipelineRFC.fit(trainingData)

      // predict
      val predictionsRFC = modelRFC.transform(testData)

      predictionsRFC.select("predictedLabel", "label", "features").show(5,truncate = false)

      val rfModelC = modelRFC.stages(2).asInstanceOf[RandomForestClassificationModel]
      rfModelC.featureImportances
      println("Learned classification forest model: \n" + rfModelC.toDebugString)

      /**
       * # RF FOR REGRESSION
       */
      val rfR = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures")
      val pipelineRFR = new Pipeline().setStages(Array(featureIndexer, rfR))
      val modelRFR = pipelineRFR.fit(trainingData)
      val predictionsRFR = modelRFR.transform(testData)
      predictionsRFR.select("prediction","label","features").show(5,truncate = false)

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
