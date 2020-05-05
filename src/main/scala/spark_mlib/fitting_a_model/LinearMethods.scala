package spark_mlib.fitting_a_model

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.regression.LinearRegression

object LinearMethods {
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
       * ### LINEAR METHODS
       * 	- Logistic Regression
       * 	- Linear Least Squares
       *
       * # LOGISTIC REGRESSION
       * 	- Widely used to predict binary responses(classification)
       * 	- Can be generalized into multinomial logistic regression(multiclass)
       * 		- for K possible outcomes, choose one outcome as "pivot"
       * 	- the other K-1 outcomes can be separately regressed against the pivot outcomes
       *
       * # LOGISTIC REGRESSION ADVANTAGES
       * 	- Has no tuning parameters
       * 	- Its prediction equation is simple and easy to implement
       *
       * # MLlib's IMPLEMENTATION
       * 	- MLlib chooses first class(0) as the "pivot" class
       * 	- For multiclass classification problem, outputs a multinomial logistic regression model,
       * containing K-1 binary logistic regression models regressed against the "pivot" class
       * 	- For a new data point, K-1 models are run and the class with largest probability is
       * choosen as the predicted class
       * 	- Supported algrithms:
       * 		- mini-batch gradient descent
       * 		- L-BFGS(recommended fof faster convergence)
       *
       * # REGULARIZATION
       * 	- L2 regularization: Ridge Regression(penalizes beta parameters by the square of their magnitude)
       * 	- L1 regularization: Lasso(penalizes beta parameters by their absolute value)
       * 	- Elastic net regularization combines L1 and L2, with a wight for each
       * 		- equivalent to ridge regression(L2) if alfa set to 0
       * 		- equivalent to Lasso(L1) if alfa set to 1
       *
       * # ELASTIC NET REGULARIZATION: PARAMETERS
       * 	- elasticNetParam corresponds to alfa
       * 	- regParam correspond to lambda
       *
       * regularizer R(w)			gradient or sub-gradient
       * zero(unregularized)	0					0
       * L2			1/2||w||22				w
       * L1			||w||1					sign(w)
       * elastic net		alfa||w||1+(1 - alfa)1/2||w||22		alfa sign(w) + (1 - alfa)w
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

      /**
       * # EXAMPLE OF LOGISTIC REGRESSION
       */
      val logr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
      val logrModel = logr.fit(trainingData)
      println(s"Weights: ${logrModel.coefficients} Intercept: ${logrModel.intercept}")

      val trainingSummaryLR = logrModel.summary

      val objectiveHistoryLR = trainingSummaryLR.objectiveHistory
      println(s"objectHistory: ${objectiveHistoryLR.mkString("\n")}")

      /**
       * # LINEAR LEAST SQUARES
       * 	- Most common formulation for regression problems
       * 	- As in logistic regression, different types of regularization are possible:
       * 		- no regularization: Ordinary Least Squares
       * 		- L2 regularization: Ridge Regression
       * 		- L1 regularization: Lasso
       * 		- Elastic net
       * 	- Average loss = Mean Squared Error
       */
      /**
       * # Example of Linear Regression
       */
      val lr = new LinearRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)

      val lrModel = lr.fit(trainingData)

      println(s"Weights: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

      val trainingSummaryLLS = lrModel.summary

      println(s"numIterations: ${trainingSummaryLLS.totalIterations}")

      println(s"objectHistory: ${trainingSummaryLLS.objectiveHistory.toList}")

      trainingSummaryLLS.residuals.show(3)

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
