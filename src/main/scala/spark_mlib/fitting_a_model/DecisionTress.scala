package spark_mlib.fitting_a_model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

object DecisionTress {
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
       * ### DECISION TREES
       * 	- Popular method for classification an regression
       * 	- Easy to interpret
       * 	- Handle categorical features
       * 	- Extend to multiclass classification
       * 	- Do NOT require feature scaling
       * 	- They capture non-linearities and feature interactions
       *
       * # MLLIB'S IMPLEMENTATION
       * 	- Supports binary and multiclass classification
       * 	- Supports regression
       * 	- Supports continuous and categorical features
       * 	- Partitions data by rows, allowing distributed training
       * 	- Used by the Pipelines API for Decision Trees
       *
       * # SPARK.ML API FOR DECISION TREES
       * 	- More functionalities than the original MLlib API
       * 	- Separation of Decision Trees for classification and regression
       * 	- Use of DF metadata to distinguish between continuous and categorical features
       * 	- For classification trees
       * 		- class conditional probabilities that is predicted
       * probabilities of each class, made available
       * 	- estimates of feature importance
       *
       * # INPUTS AND OUTPUTS
       *
       * Param name	Type(s)		Default		Description
       *
       * labelCol	Double		"label"		"Label to predict"
       * featuresCol	Vector		"features"	"Feature vector"
       *
       *
       * Param name		Type(s)		Default			Description		Notes
       * predictionCol		Double		"prediction"		Predicted label
       * rawPredictionCol	Vector		"rawPrediction"		Vector of length #	Classification only
       * with the counts of
       * training instance
       * labels at the tree
       * node which makes the
       * prediction
       *
       * probabilityCol		Vector		"probability"		Vector of length #	Classification only
       * classes equal to
       * rawPrediction
       * normalized to a
       * multinomial distribution
       */

      /**
       * # LOADING DATA
       */
      import spark.implicits._

      val dtC = new DecisionTreeClassifier().setLabelCol("indexedLabel")
        .setFeaturesCol("indexedFeatures")

      // get the data into /tmp
      // : sh wget -P /tmp https://raw.githubusercontent.com/apache/spark/master/data/mllib/sample_libsvm_data.txt

      val data = MLUtils.loadLibSVMFile(sc, "/tmp/sample_libsvm_data.txt").map(f => (f.label, f.features.asML)).toDF("label", "features")

      data.show(5, truncate = false)

      /**
       * # CREATING THE TREE MODEL
       */

      //type struct<type:tinyint,size:int,indices:array<int>,values:array<double>> but was actually
      //   struct<type:tinyint,size:int,indices:array<int>,values:array<double>>
      val labelIndexer = new StringIndexer().setInputCol("label")
        .setOutputCol("indexedLabel").fit(data)

      val labelConverter = new IndexToString().setInputCol("prediction")
        .setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

      val featureIndexer = new VectorIndexer().setInputCol("features")
        .setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

      val pipelineClass = new Pipeline()
        .setStages(Array(labelIndexer, featureIndexer, dtC, labelConverter))

      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

      /**
       * # DECISIONTREECLASSIFIER
       */
      val modelClassifier = pipelineClass.fit(trainingData)
      val treeModel = modelClassifier.stages(2).asInstanceOf[DecisionTreeClassificationModel]

      println("Learned classification tree model: \n" + treeModel.toDebugString)

      // Getting results from the model
      val predictionsClass = modelClassifier.transform(testData)
      predictionsClass.show(5, truncate = false)

      /**
       * # DECISIONTREEREGRESSOR
       */

      val dtR = new DecisionTreeRegressor().setLabelCol("label")
        .setFeaturesCol("indexedFeatures")

      val pipelineReg = new Pipeline().setStages(Array(featureIndexer, dtR))

      val modelRegressor = pipelineReg.fit(trainingData)
      val treeModel1 = modelRegressor.stages(1).asInstanceOf[DecisionTreeRegressionModel]

      println("Learned regression tree model:\n" + treeModel1.toDebugString)

      // Getting results from the model
      val predictionsReg = modelRegressor.transform(testData)
      predictionsReg.show(5, truncate = false)

      /**
       * # PROBLEM SPECIFICATION PARAMETERS
       * 	- Describe the problem and the dataset
       * 	- Should be specified
       * 	- Do not require tuning
       * 	- Parameters:
       * 		- numClasses: number of classes(classification)
       * 		- categoricalFeaturesInfo: specifies which features are categorical and how
       * many categorical values each of those features can take
       * 			- optional: if not specified, algorithm may still get reasonable results
       * 			- BUT performance should be better if categorical features are designated
       * 			- map from feature indices to number of categories
       * 			- features not in the map are treated as continuous
       *
       * # STOPPING CRITERIA
       * 	- Determine when the tree stops building
       * 	- May lead to overfitting
       * 	- Need to be validate on held-out test data
       *
       * # STOPPING CRITERIA, PARAMETERS
       * 	- maxDepth: maximum depth of a tree
       * 		- if it increases(deeper trees):
       * 			- more expressive, potentially higher accuracy
       * 			- more costly to train
       * 			- more likely overfit
       * 	- minInstancesPerNode: each child must receive at least this
       * number of instances for a node to be split further
       * 		- commonly used in Random Forest as its trees are deeper and may overfit.
       * 	- minInfoGain: the split must improve this much, in terms of information gain,
       * for a node to be split further
       * 		- The information gain is the difference between the parent node
       * impurity and the weighted sum of the two child node impurities
       * 		- Node impurity is a measure of the homogeneity of the labels at the node.
       *
       * # TUNABLE PARAMETERS I
       * 	- maxBins: number of bins used when discretizing continuous features
       * 		- must be at least the maximum number of categories for any categorical feature
       * 		- if it increases:
       * 			- allows the consideration of more split candidates and
       * fine-grained split decisions
       * 		- increases computation and communication
       *
       * # TUNABLE PARAMETERS II
       * 	- maxMemoryInMB: amount of memory to be used for collecting sufficient statistics
       * 		- default = 256MB, works in most scenarios
       * 		- if it increases:
       * 			- can lead to faster training by allowing fewer passes over data
       * 			- there may be decreasing returns since amount of communication on each
       * interaction also increases
       *
       *
       * # TUNABLE PARAMETERS III
       * 	- subsamplingRate: fraction of the training data used for learning the decision tree
       * 		- more relevant for training ensemblers of trees
       * 	- impurity: impurity measure used to choose between candidate splits
       * 		- classification: Gini impurity and Entropy
       * 		- regression: Variance
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
