package spark_mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row,SparkSession}

// In this case we are using Kmeans algorithm.
// Clustering is the best-known type of unsupervised learning.
// Clustering algorithms try to find natural grouping in data.
// These are data points that are like one another, but unlike others,
//  are likely to represent a meaningful grouping, and so clustering
//  algorithms try to put such data into the same cluster.
// This example is also taken from Spark distribution and simplified
object UnsupervisedLearningExample {

  val FEATURES_COL = "features"

  val spark = SparkSession
    .builder()
    .appName("UnsupervisedLearning")
    .master("local[*]")
    .config("spark.app.id", "Kmeans") // To silence Metrics warning.
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = run("data/kmeans_data.txt", 3)

  def run(input: String, k: Int): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    // Loads data
    val rowRDD = sc.textFile(input)
      .filter(n => n.nonEmpty)
      .map(line => line.split(" ").map(n => n.toDouble))
      // Vectors is the input type KMeans algo expects. It represents a
      // numeric vector o features that algo will use to create cluster.
      .map(Vectors.dense)
      .map(Row(_)) // Each element in Dataframe is represented as Row, think of it as database row
      .cache()
    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT , false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)

    // Train a k-means model
    val kmeans = new KMeans()
      .setK(k)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)
    val model: KMeansModel = kmeans.fit(dataset)

    // Show the result
    println("Final Centers: ")
    model.clusterCenters.zipWithIndex.foreach(println)

    // Now predict centers for following test data
    //  centers tells which groups these test data belongs
    val testData = sc.parallelize(Seq("0.3 0.3 0.3","8.0 8.0 8.0","8.0 0.1 0.1"))
      .flatMap(line => line.split(" "))
      .map(line => line.split(" ").map(n => n.toDouble))
      .map(Vectors.dense)
    val test = testData.map(Row(_))
    val testDF = sqlContext.createDataFrame(test, schema)

    model.transform(testDF).show()

  }

}