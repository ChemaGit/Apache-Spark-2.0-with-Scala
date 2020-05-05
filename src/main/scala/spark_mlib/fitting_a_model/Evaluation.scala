package spark_mlib.fitting_a_model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.regression.{LinearRegression, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
object Evaluation {
  val spark = SparkSession
    .builder()
    .appName("LinearMethods")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","LinearMethods") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      /**
       * # EVALUATORS
       * 	- Computes metrics from predictions
       * 	- Available Evaluators
       * 		- BinaryClassificationEvaluator
       * 		- MultiClassClassificationEvaluator
       * 		- RegressionEvaluator
       */
      /**
       * LOADING DATA
       */
      import sqlContext.implicits._

      // get the data into /tmp
      // wget -P /tmp https://raw.githubusercontent.com/apache/spark/master/data/mllib/sample_libsvm_data.txt

      val data = MLUtils.loadLibSVMFile(sc, "/tmp/sample_libsvm_data.txt")
        .map(f => (f.label, f.features.asML))
        .toDF("label", "features")

      data.show(5,truncate = false)

      val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

      /**
       * # EXAMPLE OF LOGISTIC REGRESSION
       */
      val logr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      val logrModel = logr.fit(trainingData)
      println(s"Weights: ${logrModel.coefficients} Intercept: ${logrModel.intercept}")

      /**
       * # BINARY CLASSIFICATION EVALUATOR
       * 	- Evaluator for binary classification
       * 	- Expects two input columns: rawPrediction and label
       * 	- Supported metric: areaUnderROC
       */
      val predictionsLogR = logrModel.transform(testData)

      val evaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("rawPrediction")
        .setMetricName("areaUnderROC")

      // this is close-to-perfect model
      val roc = evaluator.evaluate(predictionsLogR)
      println(s"roc: $roc")

      /**
       * # MULTICLASS CLASSIFICATION EVALUATOR
       * 	- Expects two input columns: prediction and label
       * 	- Supported metrics:
       * 		- F1(default)
       * 		- Precision
       * 		- Recall
       * 		- weightedPrecision
       * 		- weightedRecall
       *
       * # REUSING RF CLASSIFICATION EXAMPLE
       */
      val labelIndexer = new StringIndexer().setInputCol("label")
        .setOutputCol("indexedLabel").fit(data)

      val labelConverter = new IndexToString().setInputCol("prediction")
        .setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

      val featureIndexer = new VectorIndexer().setInputCol("features")
        .setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

      val rfC = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(3)

      val pipelineRFC = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rfC, labelConverter))
      val modelRFC = pipelineRFC.fit(trainingData)

      // predict
      val predictionsRFC = modelRFC.transform(testData)
      predictionsRFC.show(3, truncate = false)

      /**
       * # MULTICLASS CLASSIFICATION EVALUATOR
       */
      val evaluator1 = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction") //.setMetricName("precision")

      val accuracy = evaluator1.evaluate(predictionsRFC)

      println("Test Error = " + (1.0 - accuracy))

      /**
       * # REGRESSION EVALUATOR
       * 	- Evaluator for regression
       * 	- Expects two input columns: prediction and label
       * 	- Supported metrics:
       * 		- rmse: root mean squared error(default)
       * 		- mse: mean squared error
       * 		- mae: mean absolute error
       *
       * # EXAMPLE OF REGRESSION EVALUATOR
       */
      val rfR = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures")
      val pipelineRFR = new Pipeline().setStages(Array(featureIndexer, rfR))
      val modelRFR = pipelineRFR.fit(trainingData)
      val predictionsRFR = modelRFR.transform(testData)
      predictionsRFC.show(3, truncate = false)

      val evaluator2 = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction").setMetricName("rmse")

      val rmse = evaluator2.evaluate(predictionsRFR)

      println("Root Mean Squared Error (RMSE) = " + rmse)

      /**
       * # BINARY LOGISTIC REGRESSION SUMMARY
       * 	- LogisticRegressionTrainingSummary accesible through summary attribute of a LogisticRegressionModel
       * 	- Summarizes the model over the training set
       * 	- Can be casted as BinaryLogisticRegressionSummary
       *
       * 	- Supported metrics:
       * 		- areaUnderROC: area under the receiver operating characteristic(ROC) curve
       * 		- fMeasureByThreshold: dataframe with two fields(threshold, F-Measure) curve with beta=1
       * 		- pr: precision-recall curve, dataframe containing two fields recall, precision with(0.0,1.0) prepended to it
       * 		- precisionByThreshold: dataframe with two fields(threshold, precision) curve
       * 		- recallByThreshold: dataframe with two fields (threshold, recall) curve
       * 		- roc: receiver operating characteristic(ROC) curve, dataframe having two fields(FPR,TPR)
       * with(0.0,0.0) prepended and (1.0,1.0) appended to it
       *
       * # A SIMPLE BINARY LOGISTIC REGRESSION SUMMARY
       */
      val trainingSummaryLR = logrModel.summary
      val binarySummary = trainingSummaryLR
        .asInstanceOf[BinaryLogisticRegressionSummary]
      println(binarySummary.areaUnderROC)

      val fMeasure = binarySummary.fMeasureByThreshold
      fMeasure.show(3)

      val maxFMeasure = fMeasure.agg("F-Measure" -> "max")
        .head().getDouble(0)

      val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
        .select("threshold").head().getDouble(0)
      println(bestThreshold)

      binarySummary.pr.show(3)
      binarySummary.precisionByThreshold.show(3)
      binarySummary.recallByThreshold.show(3)
      binarySummary.roc.show(3)

      /**
       * # LINEAR REGRESSION TRAINING SUMMARY
       * 	- Accesible through summary attribute of a LinearRegressionModel
       * 	- Summarizes the model over the training set
       * 	- Supported metrics:
       * 		- explainedVariance: explained variance regression score
       * 		- meanAbsoluteError: mean absolute error (L1-norm loss)
       * 		- meanSquaredError: mean squared error(quadratic loss)
       * 		- r2:R2, the coefficient of determination
       * 		- residuals: residuals(label-predicted value)
       * 		- rootMeanSquaredError: root mean squared error
       *
       * # LINEAR REGRESSION EXAMPLE
       */
      val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      val lrModel = lr.fit(trainingData)

      val trainingSummaryLLS = lrModel.summary
      println(trainingSummaryLLS.explainedVariance)
      println(trainingSummaryLLS.meanAbsoluteError)
      println(trainingSummaryLLS.meanSquaredError)
      println(trainingSummaryLLS.r2)

      trainingSummaryLLS.residuals.show(3)
      println(trainingSummaryLLS.rootMeanSquaredError)

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
