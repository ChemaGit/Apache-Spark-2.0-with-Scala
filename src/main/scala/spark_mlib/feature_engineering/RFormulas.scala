package spark_mlib.feature_engineering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RFormulas {

  val spark = SparkSession
    .builder()
    .appName("RFormulas")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "RFormulas") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {
      /**
       * # RFORMULA
       * 	- In modelling we often define a variable (criterion) as a
       * function of other variables(predictors)
       * 	- The pseudocode for this(very close to more traditional math notation) could be:
       * Variable Y = variable A ... AND variable z
       *
       * 	- But wait! The = symbol is not really appropriate because it's not
       * representating the idea well. Left and right sides are not equal
       * (we wish! Then we could do perfet predictions!).
       * We want a symbol that represents the idea that 'Y is a function of A...Z'.
       * If we were scribbling on paper, a good candidate for such symbol could be '~'
       * Variable Y ~ variable A ... AND variable z
       *
       * # SYMBOLS
       * 	- The AND symbol is not really appropiate because there are different ways variables A..Z
       * can be combined to explain Y. One simple way could be just adding each variable
       * VAriable ~ variable A + variable B ... + variable Z
       * 	- Note that this is not the vanilla '+' symbol; in the context of a formula '+' has a different
       * meaning to standard '+'. It means 'add A as a predictor in this model'
       *
       * # WAYS VARIABLES CAN BE COMBINED
       *
       * Symbol	meaning					example
       * ~	as a function of			y ~ a
       * +	Add the variable to the model		y ~ a + b + c
       * :	Interaction				y ~ a + b + c + a:b
       * *	Factor crossing				y ~ a * b is interpreted as
       * y ~ a + b + a:b
       *
       * # INTERNALS I
       * 	- An RFormula object produces a vector column of features and a double
       * column of labels
       * 	- String input columns will be one-hot encoded
       * (remember lesson 3.2.2, categorical features),and
       * numeric columns will be cast to doubles
       */

      /**
       * # Loading the CSV Into a Dataframe
       *
       * :dp com.databricks % spark-csv_2.10 % 1.2.0
       *
       * :wget -P /tmp https://s3.eu-central-1.amazonws.com/dsr-data/UScrime/UScrime2-colsLotsOfNAremoved.csv
       */
      val crimes = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("deliminter", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/tmp/UScrime2-colsLotsOfNAremoved.csv")

      /**
       * # REAL DATA EXAMPLE
       */

      // define a model with RFormula interface
      import org.apache.spark.ml.feature.RFormula

      val formula = new RFormula()
        .setFormula("ViolentCrimesPerPop ~ householdsize + PctLess9thGrade + pctWWage")
        .setFeaturesCol("features")
        .setLabelCol("label")

      // Run the model and show the output
      // This is a regression model, so 'label' is a bit misleading
      val output = formula.fit(crimes).transform(crimes)

      output.select("features", "label").show(3)

      /**
       * # ADVANTAGES
       *       - You can see how RFormula is a very handy shorthand to write quite elaborated models
       *       - It started in the R world used only on linear models. With time, other
       * models(example, random forests) started using formula notation to specify models.
       *       - RFormula simplifies the creation of ML pipelines by providing a concise
       * way of expressing complex feature transformations
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
