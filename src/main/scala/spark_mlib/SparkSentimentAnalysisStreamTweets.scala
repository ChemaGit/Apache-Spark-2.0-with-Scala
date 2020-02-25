package spark_mlib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.Logging

/**
Generally, regression refers to predicting a
numeric quantity like size or income or temperature,
while classification refers to predicting a label
or category, like "spam" or "picture of a cat."
Naive bayes is a classification algorithm
  */
object SparkSentimentAnalysisStreamTweets {
  val spark = SparkSession
    .builder()
    .appName("SparkSentimentAnalysisStreamTweets")
    .master("local[*]")
    .config("spark.app.id", "SparkSentimentAnalysisStreamTweets") // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext

  val sqlContext = spark.sqlContext

  def getTwitterStream(twitterApiKey: String, twitterApiSecret: String, twitterTokenKey: String, twitterTokenSecret: String) = {
    val builder = new ConfigurationBuilder()
    builder.setOAuthConsumerKey(twitterApiKey)
    builder.setOAuthConsumerSecret(twitterApiSecret)
    builder.setOAuthAccessToken(twitterTokenKey)
    builder.setOAuthAccessTokenSecret(twitterTokenSecret)
    val configuration = builder.build()
    configuration
  }

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)

    // read twitter tokens from arguments
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args

    val ssc = new StreamingContext(sc, Seconds(5))

    try {
      val bayesModel: NaiveBayesModel = createModel(sqlContext)

      val configuration = getTwitterStream(consumerKey, consumerSecret ,accessToken,accessTokenSecret)

      val stream: DStream[Status] = TwitterUtils.createStream(ssc,Some(new OAuthAuthorization(configuration)) , Seq("programming", "computer", "crash") )//filter criteria

      stream
        .filter(s => s.getUser.getLang == "en")
        .filter(s => !s.isPossiblySensitive)
        .map(s => (s.getUser.getName, s.getText))
        .foreachRDD {rdd =>
          // We will use hashing to translate words to numeric features
          val htf: HashingTF = new HashingTF()
          // this is the power of Spark where we are able to use algo. written for batches
          // addresses the Lambda architecture problems
          rdd.map {
            case(username, text) => (bayesModel.predict(htf.transform(text.split(" "))), username)
          }.foreach(println)
        }

      ssc.start()
      ssc.awaitTerminationOrTimeout(1000 * 600) // run for 60 seconds

    } finally {
      spark.stop()
    }
  }

  def createModel(sqlContext: SQLContext): NaiveBayesModel = {

    val htf = new HashingTF()

    // Data format:
    //	0 - the polarity of the tweet (0 = negative, 2 = neutral, 4 = positive)
    //	1 - the id of the tweet (2087)
    //	2 - the date of the tweet (Sat May 16 23:58:44 UTC 2019)
    //	3 - the query (lyx). If the is no query, then this values is NO_QUERY.
    //	4 - the user that tweeted (robotickilldozr)
    //	5 - the text of the tweet (Lyx is cool)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load("/home/cloudera/CognitiveClass/data/data_twitter/disk4.csv") // training data taken from help.sentiment140.com
      .toDF("polarity", "id", "date", "query", "user", "tweet")

    df.printSchema()
    df.show()

    df.createOrReplaceTempView("tw")
    val aux = sqlContext
      .sql("""SELECT polarity, tweet FROM tw WHERE polarity IS NOT NULL AND tweet IS NOT NULL""")

    val labledRdd: RDD[LabeledPoint] = aux
      .rdd
      .filter(r => r.length == 2)
      .filter(r => r(1).toString.nonEmpty)
      .map {
        case Row(polarity: Int, tweet: String) =>
          val words = tweet.split(" ")
          LabeledPoint(polarity, htf.transform(words))

      }

    labledRdd.cache()

    // We are using Multinomial Naive Bayes variation by default
    val bayesModel: NaiveBayesModel = NaiveBayes.train(labledRdd)
    bayesModel
  }
}