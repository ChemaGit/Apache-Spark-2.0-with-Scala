package spark_mlib.pipeline_and_grid_search

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.SparkSession

object PredictingGrantApplication {
  val spark = SparkSession
    .builder()
    .appName("PredictingGrantApplication")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","PredictingGrantApplication") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  implicit class BestParamMapCrossValidatorModel(cvModel: CrossValidatorModel) {
    def bestEstimatorParamMap: ParamMap = cvModel.getEstimatorParamMaps.zip(cvModel.avgMetrics).maxBy(_._2)._1
  }

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      /**
       * # THE DATASET AND PROBLEM
       * 	- Dataset
       * 		- 8708 grant applications from the University of Melbourne, 3992 of which were successful
       * 		- the 17594 researchers involved in those applications
       * 		- https://www.kaggle.com/c/unimelb
       * 	- Problem
       * 		- Predict if a grant application will be successful
       *
       * # DATA ABOUT GRANTS
       * 	- Grant_Application_ID(int)
       * 	- Grant_Status(int)
       * 		- Whether or not the grant application was successful
       * 	- Contract_Value_Band(string)
       * 		- The amount of the grant, bucketized into ranges
       * 	- Grant_Category_Code(string)
       * 		- Categorization of the sponsor(e.g. Australian competitive grants, cooperative research centre, industry)
       *
       * # DATA ABOUT RESEARCHERS
       * 	- Role(string)
       * 		- The individual researcher's role in the proposed research
       * 	- Year_of_Birth(int)
       * 	- Dept_No(string)
       * 	- With_PHD(string)
       * 	- Number_of_Successful_Grant(int)
       * 	- Number_of_Unsuccessful_Grant(int)
       * 	- A2, A, B, C(four int columns)
       * 		- Number of articles published in journals of various
       * quality(A2 being the most prestigious, C the least)
       */
      /**
       * # BASIC SETUP
       * // :dp libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
       * // :sh wget -P /tmp https://s3.eu-central-1-amazonaws.com/dsr-data/grants/grantsPeople.csv
       */
      /**
       * LOADING THE DATA
       */
      import spark.implicits._
      import org.apache.spark.sql.functions._

      val data = spark.read.
        format("com.databricks.spark.csv").
        option("delimiter", "\t").
        option("header", "true").
        option("inferSchema", "true").
        load("/tmp/grantsPeople.csv")

      data.show(5, truncate = false)

      /**
       * # PLAN OF ATTACK?
       * 	- Think about
       * 		- We want to have features that are good predictors for each grant
       * 		- How can we create features about grants from the data about the researches?
       * 		- Which values in the dataset might make good predictors?
       */


      /**
       * ### PREDICTIONG GRANT APPLICATIONS - CREATING FEATURES
       *
       * # OVERALL PLAN OF ATTACK
       * 	- Make some of the data more useful by adding some columns to the dataset that re-encode a few fields
       * 	- Using groupBy on the Grant_application_ID, aggregate/summarize the data about researchers
       * 	- When necessary, create StringIndexers for the categorical features
       */
      /**
       * # RE-ENCODE SOME DATA
       */
      val researchers = data.
        withColumn ("phd", data("With_PHD").equalTo("Yes").cast("Int")).
        withColumn ("CI", data("Role").equalTo("CHIEF_INVESTIGATOR").cast("Int")).
        withColumn("paperscore", data("A2") * 4 + data("A") * 3)

      researchers.show(5, truncate = false)

      /**
       * # SUMMARIZE TEAM DATA
       */
      val grants = researchers.groupBy("Grant_Application_ID").agg(
        max("Grant_Status").as("Grant_Status"),
        max("Grant_Category_Code").as("Category_Code"),
        max("Contract_Value_Band").as("Value_Band"),
        sum("phd").as("PHDs"),
        when(max(expr("paperscore * CI")).isNull, 0).
          otherwise(max(expr("paperscore * CI"))).as("paperscore"),
        count("*").as("teamsize"),
        when(sum("Number_of_Successful_Grant").isNull, 0).
          otherwise(sum("Number_of_Successful_Grant")).as("successes"),
        when(sum("Number_of_Unsuccessful_Grant").isNull, 0).
          otherwise(sum("Number_of_Unsuccessful_Grant")).as("failures")
      )

      grants.show(5, truncate = false)

      /**
       * # HANDLE CATEGORICAL FEATURES
       */
      import org.apache.spark.ml.feature.StringIndexer

      val value_band_indexer = new StringIndexer().
        setInputCol("Value_Band").
        setOutputCol("Value_index").
        fit(grants)

      val category_indexer = new StringIndexer().
        setInputCol("Category_Code").
        setOutputCol("Category_index").
        fit(grants)

      val label_indexer = new StringIndexer().
        setInputCol("Grant_Status").
        setOutputCol("status").
        fit(grants)

      /**
       * # GATHER THE FEATURES INTO A VECTOR
       */
      import org.apache.spark.ml.feature.VectorAssembler

      val assembler = new VectorAssembler().
        setInputCols(Array(
          "Value_index"
          ,"Category_index"
          ,"PHDs"
          ,"paperscore"
          ,"teamsize"
          ,"successes"
          ,"failures"
        )).setOutputCol("assembled")

      /**
       * # SETUP A CLASSIFIER
       */
      import org.apache.spark.ml.classification.RandomForestClassifier
      import org.apache.spark.ml.classification.RandomForestClassificationModel

      val rf = new RandomForestClassifier().
        setFeaturesCol("assembled").
        setLabelCol("status").
        setSeed(42)

      println(rf.explainParams)

      /**
       * ### PREDICTING GRANT APPLICATIONS - BUILDING A PIPELINE
       *
       * # KEY CONCEPTS
       * 	- Transformers
       * 		- an algorithm which transforms one DataFrame into another
       * 	- Estimator
       * 		- an algorithm which can be fit on a DataFrame to produce a Transformer
       * 	- Parameter
       * 		- there is a common API shared by Transformers and Estimators
       * 	- Pipeline
       * 		- chains multiple Transformers together to specify a machine learning workflow
       * 	- Evaluator
       * 		- measures the performance of an estimator or pipeline against some metric(s)
       *
       * # PIPELINES IN SPARK.ML
       * 	- Inspired by the scikit-learn project
       * 	- Components:
       * 		- Transformers
       * 		- Estimators
       * 	- Properties of components:
       * 		- Transformers.transform() and Estimator.fit() are stateless
       * 	- Each instance of Transformer/Estimator has a unique ID
       *
       * 	- A sequence of PipelineStages to be run in a specific order
       * 		- input DataFrame is transformed as it passes through each stage
       * 		- Transformers stages: transform() method is called on the DF
       * 		- Estimators stages: fit() method is called to produce a Transformer
       * 			- This transformer becomes part of the PipelineModel
       * 			- transform() method is called on the DF
       * 	- Runtime checking is done using the DF's schema before actually running the Pipeline
       *
       * # CREATE THE PIPELINE
       */
      import org.apache.spark.ml.Pipeline
      val pipeline = new Pipeline().setStages(Array(
        value_band_indexer,
        category_indexer,
        label_indexer,
        assembler,
        rf)
      )

      /**
       * # CREATE AN EVALUATOR
       */
      import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
      val auc_eval = new BinaryClassificationEvaluator().
        setLabelCol("status").
        setRawPredictionCol("rawPrediction")

      println(auc_eval.getMetricName)

      /**
       * # SPLIT INTO TRANINIG AND TEST
       */
      val tr = grants.filter("Grant_Application_ID < 6635")
      val te = grants.filter("Grant_Application_ID >= 6635")
      val training = tr.na.fill(0, Seq("PHDs"))
      val test = te.na.fill(0, Seq("PHDs"))

      println((training.count, test.count))

      /**
       * # RUN AND EVALUATE THE PIPELINE
       */
      val model = pipeline.fit(training)
      val pipeline_results = model.transform(test)
      println(auc_eval.evaluate(pipeline_results))

      /**
       * ### PREDICTING GRANT APPLICATIONS - CROSS VALIDATION AND MODEL TUNING
       *
       * # TYPICAL WORKFLOW
       *
       * DataFrame			Transformer					Estimator				Evaluator
       *
       * Load data	-->		Extract Features		-->		Train model		-->		Evaluate model
       * labels,text,numbers			labels,feature vectors				labels,predictions
       *
       *
       * We need an organized way to explore the parameter space for these Transformers and Estimators
       *
       * # CHOOSING PARAMETERS FOR TUNING
       */
      println(rf.extractParamMap)

      /**
       * They are rather small, they were probably chosen so that models could be built
       * quickly, by default
       * // rfc_maxDepth: 5
       * // rfc_numTrees: 20
       *
       * # SIMPLY GRID SEARCH
       */
      import org.apache.spark.ml.tuning.ParamGridBuilder

      val paramGrid = new ParamGridBuilder().
        addGrid(rf.maxDepth, Array(10, 30)).
        addGrid(rf.numTrees, Array(10, 100)).
        build()

      /**
       * # A SMALL COMBINATORIAL EXPLOSION
       *
       * # CROSS VALIDATION
       * 	- Main idea: test with data not used for traninig
       * 	- Split the data several times
       * 	- Each time, use part of the data for training and the rest for testing
       *
       * # K-FOLD CROSS VALIDATION
       * 	- Spark supports k-fold cross validation
       * 		- Divides the data into k non-overlapping sub-examples
       * 		- Performance is measured by averaging the result of th Evaluator across k folds
       * 		- k should be at least 3
       *
       * DATA 	test - traninig	-->
       * test		-->	average
       * test	-->
       */
      import org.apache.spark.ml.tuning.CrossValidator

      val cv = new CrossValidator().
        setEstimator(pipeline).
        setEvaluator(auc_eval).
        setEstimatorParamMaps(paramGrid).
        setNumFolds(3)

      /**
       * # FINAL RESULTS
       */
      val cvModel = cv.fit(training)

      val cv_results = cvModel.transform(test)

      // with the default parameters we got 0.908

      println(auc_eval.evaluate(cv_results))

      println(cvModel.avgMetrics)

      /**
       * # BOTTOM LINE
       * 	- Nice improvement with little work on our part
       * 		- 0.925 > 0.908
       * 	- This did require 12x the computational effort
       * 		- 3x because of the 3-fold cross-validation
       * 		- 4x because of parameter grid search
       * 	- But it's embarrassingly parallelizable
       * 		- this is where spark.ml really shines
       */

      /**
       * # Finding the Winning Parameters
       */

      /**
       * # Using bestEstimatorParamMap
       */
      println(cvModel.bestEstimatorParamMap)

      /**
       * # Best Model
       */
      val bestPipelineModel = cvModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
      println(bestPipelineModel.stages)

      /**
       * # Extracting the Winning Classifier
       */
      val bestRandomForest = bestPipelineModel.stages(4).asInstanceOf[RandomForestClassificationModel]
      println(bestRandomForest.toDebugString)

      /**
       * # totalNumNodes
       */
      println(bestRandomForest.totalNumNodes)

      /**
       * # Feature Importances
       */
      println(bestRandomForest.featureImportances)

      /**
       * Wrapping Up
       * Using the default parameters, we had an area under the ROC curve of 0.909
       * After a grid search, we got that up to 0.926
       * Running the grid search on a cluster was a real timesaver
       * Not all of our features proved very useful; maybe you can do better!
       * Module Summary
       * Having completed this module about Predicting Grant Applications, you should be able to:
       * Understand how to fit together the functions available in Spark's machine learning libraries to solve real problems
       * Fit models in a fraction of the time, using a Spark cluster
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
