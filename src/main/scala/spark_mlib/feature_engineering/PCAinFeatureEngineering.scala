package spark_mlib.feature_engineering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.{Vector, Matrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object PCAinFeatureEngineering {

  val spark = SparkSession
    .builder()
    .appName("PCAinFeatureEngineering")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","PCAinFeatureEngineering") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      /**
       * ### PRINCIPAL COMPONENT ANALYSIS (PCA) IN FEATURE ENGINEERING
       *
       * # PCA: Definition
       * 	- PCA is a dimension reduction technique.
       * It is unsupervised machine learning, and it has many
       * used; on this video we only care about its use for feature engineering.
       *
       * # PCA: How it Works
       * 	- The first Principal Component(PC) is defined as the linear
       * combination of the predictors that captures the most variability
       * of all possible linear combinations.
       * 	- Then, subsequent PCs are derived such that these linear
       * combinations capture the most reamining variability while
       * also being uncorrelated with all previous PCs.
       *
       * # Feature Engineering
       * 	- "Feature engineering" is a practice where predictors are created and refined
       * to maximaze model performance
       * 	- It can take quite some time to identify and prepare relevant features.
       *
       * # Feature Engineering with PCA
       * 	- Basic idea: generate a smaller set of variables that
       * capture most of the information in the original variables
       * 	- The new predictors are functions of the original
       * predictors; all the original predictors are still needed
       * to create the surrogate variables.
       *
       * # Dataset: Predict US Crimes
       * 	- We want to predict the proportion of violent crimes per 100K population
       * on different locations in the US
       * 	- More tha 100 predictors. Examples:
       * 		- householdsize: mean people per household
       * 		- PctLess9thGrade: percentage of offenders who have not yet entered high school
       * 		- pctWWage: percentage of households wage or salary income in 1989
       * 	- For a description of the variables, see the UCI repository(communities and crimes)
       *
       * 	- Let's assume that whe don't want to operate with those > 100 predictors. Why?
       * 		- Some will be collinear(ie highly correlated)
       * 		- It's hard to see reationships in a high-dimensional space
       * 	- How do we use PCA to get down to 10 dimensions?
       */

      /**
       * # Loading the CSV Into a Dataframe
       *
       * :dp com.databricks % spark-csv_2.10 % 1.2.0
       *
       * :wget -P /tmp https://s3.eu-central-1.amazonws.com/dsr-data/UScrime/UScrime2-colsLotsOfNAremoved.csv
       */
      import spark.implicits._

      val crimes = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("deliminter", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/tmp/UScrime2-colsLotsOfNAremoved.csv")

      /**
       * # Convert from Dataframe to RowMatrix
       */
      val assembler = new VectorAssembler()
        .setInputCols(crimes.columns)
        .setOutputCol("features")

      val featuresDF =
        assembler.transform(crimes).select("features")
      val rddOfRows = featuresDF.rdd

      /**
       * # Convert the Precictors from Dataframe to RowMatrix
       */
      val rddOfVectors = rddOfRows.map(row => row.get(0).asInstanceOf[Vector])

      val mat = new RowMatrix(rddOfVectors)

      /**
       * # Compute Principal Components
       */
      val pc: Matrix = mat.computePrincipalComponents(10)

      /**
       * - Principal components are stored in a local dense matrix
       * - The matrix pc is now 10 dimensions, but it represents the variability
       * 'almost as well' as the previous 100 dimensions
       */

      /**
       * # PROS I
       * 	- Interpretability(!)
       * 	- PCA creates components that are uncorrelated, and Some
       * predictive models prefer little to no collinearity(example linear regression)
       * 	- Helps avoiding the 'curse of dimensionality': Classifiers tend to
       * overfit the training data in high dimensional spaces, so reducing
       * the number of dimensions may help
       *
       * # PROS II
       * 	- Performance. On further modeling, the computational effort often
       * depends on the number of variables. PCA gives you far fewer variables;
       * this may make any further processing more performant
       * 	- For classification problems PCA can show potential separation of
       * classes(if there is a separation).
       *
       * # CONS
       * 	- The computational effort often depends greatly on the numbers of variables
       * and the number of data records.
       * 	- PCA seeks linear combinations of predictors that maximize variability,
       * it will naturally first be drawn to summarizing predictors that retain
       * most of the variation in the data.
       *
       * # HOW MANY PRINCIPAL COMPONENTS TO USE?
       * 	- No simple answer to this question
       * 	- But there are heuristics:
       * 		- Find the elbow on the graph for dimensions by variance explained
       * 		- Set up a 'variance explained threshold' (for example, take as many
       * Principal components as needed to explain 95% of the variance)
       *
       * # TIP FOR BEST PRACTICE
       * 	- Always center and scale the predictors prior to performing PCA
       * Otherwise the predictors that have more variation will soak the top principal components.
       */

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
