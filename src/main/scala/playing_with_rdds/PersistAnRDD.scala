package playing_with_rdds

import org.apache.spark.sql._

object PersistAnRDD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Persist An RDD").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // Count web server log requests by user id
    val userReqs = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/weblogs/*2.log").
      map(line => line.split(" ")).
      map(words => (words(2),1)).
      reduceByKey((v1,v2) => v1 + v2)

    // Map account data to (userid,"lastname,firstname") pairs
    val accounts = sc.textFile("hdfs://quickstart.cloudera/user/cloudera/loudacre/data/accounts/*").
      map(line => line.split(",")).
      map(values => (values(0),values(4) + ',' + values(3)))

    // Join account names with request counts
    val accountHits = accounts.join(userReqs).map(pair => pair._2)

    // Solution starts here

    // How many accounts had hitcount > 5?
    val count = accountHits.filter(pair => pair._2 > 5).count()
    println(s"Count hitcount > 5: $count")

    // Persist
    accountHits.persist()

    // rerun the job
    accountHits.filter(pair => pair._2 > 5).count()
    val count2 = accountHits.filter(pair => pair._2 > 5).count()
    println(s"Count hitcount > 5: $count2")

    println(accountHits.toDebugString)

    // rerun the job
    val count3 = accountHits.filter(pair => pair._2 > 5).count()
    println(s"Count hitcount > 5: $count3")

    // Optional: Change the storage level of the RDD
    import org.apache.spark.storage.StorageLevel
    accountHits.unpersist()
    accountHits.persist(StorageLevel.DISK_ONLY)

    val count4 = accountHits.filter(pair => pair._2 > 5).count()
    println(s"Count hitcount > 5: $count4")

    println(accountHits.toDebugString)

    accountHits

    sc.stop()
    spark.stop()
  }

}
