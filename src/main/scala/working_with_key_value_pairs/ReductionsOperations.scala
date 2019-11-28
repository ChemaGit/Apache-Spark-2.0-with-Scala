package working_with_key_value_pairs

import org.apache.spark.sql.SparkSession

object ReductionsOperations {

  case class Taco(kind: String, price: Double)

  //I might only care about title and timestamp, for example. In this case, it'd save a lot of
  //time/memory to not have to carry around the full-text of each article {text) in our
  //accumulator!
  case class WikipediaPage(
                            title: String,
                            redirectTitle: String,
                            timestamp: String,
                            lastContributorUsername: String,
                            text: String)
  //Hence, why accumulate is often more desirable in Spark than in Scala collections!

  def main(args: Array[String]) {
    val  spark = SparkSession
      .builder()
      .appName("Reductions Operations")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")

    /**
      * Reduction Operations:
      * walk through a collection and combine neigboring elements of the
      * collection together to produce a single combined result.
      */

    val tacoOrder = List(Taco("Carnitas", 2.25),
      Taco("Corn", 1.75),
      Taco("Barbacoa", 2.50),
      Taco("Chicken", 2.00))
    val cost = tacoOrder.foldLeft(0.0)((sum, taco)=> sum + taco.price) //foldLeft is not parallelizable.
    println(s"cost: $cost") // cost: 8.5

    // def foldLeft[B](z: B)(f: (B, A) => B): B

    val xs = List(1, 2, 3, 4)
    val res = xs.foldLeft("")((str: String, i: Int) => str + i)
    println(s"res: $res") // What happens if we try to break this collection in two and parallelize?, It doesn't work ¡¡Type Error!!

    /**
      * def fold(z: A)(f: (A, A)=> A): A
      * It enables us to parallelize using a single function f by enabling us
      * to build parallelizable reduce trees.
      */

    /**
      * aggregate[B](z: => B)(seqop: (B, A)=> B, combop: (B, B) => B): B
      * Properties of aggregate => The best of both worlds
      * 1. Parallelizable.
      * 2. Possible to change the return type.
      */

    /**
      * Spark doesn't even give you the option to use foldLeft/foldRight.
      * Which means that if you have to change the return type of your
      * reduction operation, your only chance is to use aggregate or combine
      */

    /**
      * Reduction Operation s on RDDs
      * Scala collections                   Spark
      * fold                                fold
      * foldLeft/foldRight                  reduce
      * reduce                              aggregate
      * aggregate
      *
      * In Spark, aggregate is a more desirable reduction operator a majority of the time.
      */

    sc.stop()
    spark.stop()
  }
}
