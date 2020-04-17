package spark_mlib.preparing_data

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object HandlingMissingDataAndImputingValues {

  val spark = SparkSession
    .builder()
    .appName("HandlingMissingDataAndImputingValues")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions","4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id","HandlingMissingDataAndImputingValues") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)

    try {

      /**
       * # DATAFRAME NA FUNCTIONS
       * 	- The na method of DataFrames provides functionality for working with missing data
       * 	- Returns an instance of DataFrameNAFunctions
       * 	- The following methods are available:
       * 		- drop, for dropping rows containing NaN or null values
       * 		- fill, for replacing NaN or null values
       * 		- replace, for replacing values matching specified keys
       */

      /**
       * # SAMPLE DATAFRAME WITH MISSING VALUES
       */

      import org.apache.spark.sql.functions._

      val df = sqlContext.range(0,10).select("id").withColumn("uniform", rand(10L)).withColumn("normal", randn(10L))
      val halfToNaN = udf[Double, Double](x => if(x > 0.5) Double.NaN else x)
      val oneToNaN = udf[Double, Double](x => if(x > 1.0) Double.NaN else x)

      val dfnan = df.withColumn("nanUniform", halfToNaN(df("uniform")))
        .withColumn("nanNormal", oneToNaN(df("normal"))).drop("uniform")
        .withColumnRenamed("nanUniform", "uniform").drop("normal")
        .withColumnRenamed("nanNormal", "normal")

      dfnan.show()

      /**
       * # DATAFRAME NA FUNCTIONS - DROP
       * 	- drop is used for dropping rows containing NaN or null values according to a criteria
       * 	- Several implementations available:
       * 		- drop(minNonNulls, cols)
       * 		- drop(minNonNulls)
       * 		- drop(how, cols)
       * 		- drop(cols)
       * 		- drop(how)
       * 		- drop()
       * 	- cols is an Array or Seq of column names
       * 	- how should be equal any or all
       */

      /**
       * # Dropping Rows With minNonNulls Argument
       */
      dfnan.na.drop(minNonNulls = 3).show()

      /**
       * Dropping Rows With How Argument
       */
      dfnan.na.drop("all", Array("uniform", "normal")).show()

      dfnan.na.drop("any", Array("uniform", "normal")).show()

      /**
       * # Dataframe NA Functions - fill
       * 	- fill is used for replacing NaN or null values according to a criteria
       * 	- Several implementations available
       * 		- fill(valueMap)
       * 		- fill(value, cols)
       * 		- fill(value)
       */

      /**
       * Filling Missing Data By Column Type
       */
      dfnan.na.fill(0.0).show()

      /**
       * Filling Missing Data With Column Defaults
       */
      val uniforMean = dfnan
        .filter("uniform <> 'NaN'")
        .groupBy()
        .agg(mean("uniform"))
        .first()(0)

      dfnan.na.fill(Map("uniform" -> uniforMean)).show(5)

      val dfCols = dfnan.columns.drop(1)
      val dfMeans = dfnan.na.drop().groupBy()
        .agg(mean("uniform"), mean("normal")).first().toSeq

      val meansMap = (dfCols.zip(dfMeans)).toMap
      dfnan.na.fill(meansMap).show(5)

      /**
       * # DataFrame NA Functions - replace
       * 	- replace is used for replacing values matching specified keys
       * 	- cols argument may be a single column name or an array
       * 	- replacement argument is a map:
       * 		- key is the value to be matched
       * 		- value is the replacement value itself
       */

      /**
       * # Replacing Values in a DataFrame
       */
      dfnan.na.replace("uniform", Map(Double.NaN -> 0.0)).show()

      /**
       * # Duplicates
       * 	- dropDuplicates is a DataFrame method
       * 	- Used to remove duplicate rows
       * 	- May specify a subset of columns to check for duplicates
       */

      import sqlContext.implicits._

      val dfDuplicates = df.unionAll(sc.parallelize(Seq((10,1,1),(11,1,1))).toDF())

      /**
       * Dropping Duplicate Rows
       */
      val dfCols1 = dfnan.columns.drop(1)
      dfDuplicates.dropDuplicates(dfCols1).show()

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
