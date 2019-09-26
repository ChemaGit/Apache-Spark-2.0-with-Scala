package playing_with_rdds

import org.apache.spark.sql._

/*
Common Spark Use Cases
- Risk analysis
	- How likely is the borrower to pay back a loan?
- Recommendations
	- Which products will this customer enjoy?
- Predictions
	- How can we prevent service outages instead of simply reacting to them?
- Classification
	- How can we tell which mail is spam and which is legitimate?
- Spark examples SPARK_HOME/lib => /usr/lib/spark/lib
	- k-means
	- Logistic regression
	- Calculating pi
	- Alternating least squares(ALS)
	- Querying Apache web logs
	- Processing Twitter feeds

- Example: PageRank
	- PageRank gives web pages a ranking score based on links from other pages
		- Higher scores given for more links, and links from other high ranking pages
- PageRank is a classic example of big data analysis(like word count)
	- Lots of data: Needs an algorithm that is distributable and scalable
	- Iterative: The more iterations, the better than answer

- PageRank Algorithm
1. Start each page with a rank of 1.0
2. On each iteration:
	a. Each page contributes to its neighbors its own rank divided by the number of its neighbors: contrib = rank / neighbors
	b. Set each page's new rank based on the sum of its neighbors contribution: new_rank = sum(contrib * 0.85) + 0.15
3. Each iteration incrementally improves the page ranking

- Checkpointing
	- Maintaining RDD lineage provides resilience but can also cause problems when the lineage gets very long
		- For example: iterative algorithms, streaming
	- Recovery can be very expensive
	- Potencial stack overflow
	- Checkpointing saves the data to HDFS
		- Provides fault-tolerant storage across nodes
		- Lineage is not saved
		- Must be checkpointed before any actions on the RDD
		sc.setCheckpointDir(directory)
		.....
		myrdd.checkpoint()
*/

object CommonPattersnInSpark {
	// given the list of neighbors for a page and that page's rank, calculate
	// what that page contributes to the rank of its neighbors
	def computeContribs(neighbors: Iterable[String], rank: Double): Iterable[(String, Double)] = {
		for(neighbor <- neighbors) yield(neighbor, rank/neighbors.size)
	}

	def main(args: Array[String]): Unit = {
		val spark = SparkSession.builder().appName("Common Patterns in Apache Spark Data Processing").master("local[*]").getOrCreate()
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		// read in a file of link pairs (format: url1 url2)
		val linkfile = "hdfs://quickstart.cloudera/user/cloudera/loudacre/data/pagelinks.txt"
		val links = sc.textFile(linkfile).map(line => line.split(" ")).map(pages => (pages(0),pages(1))).distinct().groupByKey().persist()

		// create initial page ranges of 1.0 for each
		var ranks = links.map(pair => (pair._1, 1.0))

		// number of iterations
		val n = 10

		// for n iterations, calculate new page ranks based on neighbor contributions
		for(x <- 1 to n) {
			var contribs = links.join(ranks).flatMap(pair => computeContribs(pair._2._1,pair._2._2))
			ranks = contribs.reduceByKey(_ + _).map(pair => (pair._1, pair._2 * 0.85 + 0.15))
			println(s"Iteration $x")
			for(rankpair <- ranks.take(10)) println(rankpair)
			println()
		}

		sc.stop()
		spark.stop()
	}
}
