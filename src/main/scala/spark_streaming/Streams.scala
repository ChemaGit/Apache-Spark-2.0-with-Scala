package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Streams {
  val port = 9000
  val interval = Seconds(2)
  val pause = 10	// milliseconds
  val server = "127.0.0.1"
  val checkpointDir = "output/checkpoint_dir"
  val runtime = 30 * 1000	// run for N * 1000 millisecs
  val numIterations = 100000

  val spark = SparkSession
    .builder()
    .appName("Spark Streams")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.app.id", "Streams")
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    // It's a convention to use a function to construct the streaming context
    def createContext(): StreamingContext = {
      // Minibatch interval(2 seconds here)
      val ssc = new StreamingContext(sc, interval)
      ssc.checkpoint(checkpointDir)

      // Listen to a socket of text data.
      val dstream = ssc.socketTextStream(server, port)
      val numbers = for {
        line <- dstream
        number <- line.trim.split("\\s+")
      } yield number.toInt

      // For each mini-batch RDD, after construction, run this code.
      numbers.foreachRDD { rdd =>
        rdd.countByValue.foreach(println)
      }
      ssc
    }

    // To set up the data source socket, first create a Runnable
    def makeRunnable(port: Int) = new Runnable {
      def run() = {
        // Create a server socket on a port
        val listener = new java.net.ServerSocket(port)
        var socket: java.net.Socket = null
        try {
          // Accept connection
          val socket = listener.accept()
          val out = new java.io.PrintWriter(socket.getOutputStream(), true)
          // Write each to the socket, then sleep for a short "pause"
          (1 to numIterations).foreach { i =>
            val number = (100 * math.random).toInt
            out.println(number)
            Thread.sleep(pause)
          }
        } finally {
          listener.close();
          if(socket != null) socket.close()
        }
      }
    }

    def startSocketDataThread(port: Int): Thread = {
      val dataThread = new Thread(makeRunnable(port))
      // Start the source data thread
      dataThread.start()
      dataThread
    }

    // Hack: Use vars and nulls here so we can init these inside the try clause, but see them in the finally clause.
    var ssc: StreamingContext = null
    var dataThread: Thread = null

    try {
      sc.setLogLevel("ERROR")
      // Start the data source socket in a separate thread
      dataThread = startSocketDataThread(port)

      ssc = StreamingContext.getOrCreate(checkpointDir, createContext _)
      ssc.start()
      ssc.awaitTerminationOrTimeout(runtime)
    } finally {
      if(dataThread != null) dataThread.interrupt()
      if(ssc != null)
        ssc.stop(stopSparkContext = true, stopGracefully = true)
    }
  }
}