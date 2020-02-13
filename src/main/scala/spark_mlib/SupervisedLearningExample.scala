package spark_mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame,SparkSession}

case class Rating(userId: Int, movieId: Int, rating: Double)

object Rating {
  // Format is: userId, movieId, rating, timestamp
  def parseRating(str: String): Rating = {
    val fields = str.split("::")

    // We don't care about the timestamp
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
  }
}

case class Movie(movieId: Int, title: String, genres: Seq[String])

object Movie {
  // Format is: movieId, title, genre1|genre2
  def parseMovie(str: String): Movie = {
    val fields = str.split("::")
    assert(fields.size == 3)
    Movie(fields(0).toInt, fields(1), fields(2).split("|"))
  }
}

// We will take and Collaborative filtering example to rate movies.
// The data is from (http://grouplens.org/datasets/movielens/). Movielens is a non-commercial movie recommendation website
// The example is inspired from the MovieLensALS.scala example from spark distribution.
object SupervisedLearningExample {

  val numIterations = 10
  val rank = 10 // number of features to consider when training the model
  // this file in in UserID::MovieID::Rating::Timestamp format
  val ratingsFile = "data/als/sample_movielens_ratings.txt"
  val moviesFile = "data/als/sample_movielens_movies.txt"
  val testFile = "data/als/test.data"

  val spark = SparkSession
    .builder()
    .appName("SupervisedLearning")
    .master("local[*]")
    .config("spark.app.id", "") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def main(args: Array[String]): Unit = run()

  def run(): Unit = {



    import spark.implicits._

    Logger.getRootLogger.setLevel(Level.ERROR)

    // This will be our training dataset
    val ratingsData: RDD[Rating] = sc.textFile(ratingsFile).map(Rating.parseRating).cache()

    // This will be our test dataset to verify the model
    val testData: RDD[Rating] = sc.textFile(testFile).map(Rating.parseRating).cache()

    val numRatings = ratingsData.count()
    val numUsers = ratingsData.map(user => user.userId).distinct().count()
    val numMovies = ratingsData.map(movie => movie.movieId).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    // This is much more simplified version, in real world we try different rank,
    // iterations and other parameters to find best model.
    // Typically ALS model looks for two properties, usercol for user info
    // and itemcol for items that we are recommending
    val als = new ALS()
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRank(rank)
      .setMaxIter(numIterations)

    // Training the model, converting rdd to dataframe for spark.ml
    val model: ALSModel = als.fit(ratingsData.toDF)

    // Now trying the model on our testdata
    val predictions: DataFrame = model.transform(testData.toDF).cache

    // Metadata about the movies
    val movies = sc.textFile(moviesFile).map(Movie.parseMovie).toDF()

    // Try to find out if our model has any falsePositives
    // And joining with movies so that we can print the movie names
    val falsePositives = predictions.join(movies,"movieId")
      .where(($"rating" <= 1) && ($"prediction" >= 4))
      .select($"userId", predictions("movieId"), $"title", $"rating", $"prediction")
    val numFalsePositives = falsePositives.count()
    println(s"Found $numFalsePositives false positives")
    if(numFalsePositives > 0) {
      println(s"Example false positives: ")
      falsePositives.limit(100).collect().foreach(println)
    }

    // Show first 20 predictions
    predictions.show()

    // Running predictions for user 26 as an example to find
    // out whether user likes some movies
    println(">>>>> Find out predictions where user 26 likes movies 10, 15, 20 & 25")
    val df26 = sc.makeRDD(Seq(26 -> 10, 26 -> 15, 26 -> 20, 26 -> 25)).toDF("userId","movieId")
    model.transform(df26).show()

    sc.stop()
  }
}
