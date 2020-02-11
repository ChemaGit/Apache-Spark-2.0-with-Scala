package spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
// Listen for stream state transitions
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverError,StreamingListenerReceiverStopped}

import scala.util.control.NonFatal
import java.io.{File, PrintWriter}

object HiveETL {

  case class Orders(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)

  val defaultPort = 9000
  val interval = Seconds(5)
  // A forced delay during data generation
  val pause = 10 // milliseconds
  val server = "127.0.0.1"
  // Where the Hive table data will go
  val hiveETLDir = "output/hive-etl"
  val checkpointDir = "output/checkpoint_dir"
  val runtime = 30 * 1000 // run for N * 1000 millisecs
  // Write this many records, then sleep
  val numRecordsToWritePerBlock = 10000

  val spark = SparkSession
    .builder()
    .appName("ETL with Spark Streaming and Hive")
    .master("local[*]")
    .enableHiveSupport()
    .config("spark.sql.shuffle.partitions", "4") //Change to a more reasonable default number of partitions for our data
    .config("spark.app.id", "HiveETL")  // To silence Metrics warning
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext

  def deleteHiveMetadata(f: File): Boolean = {
    if(f.exists()) f.delete()
    else false
  }

  def main(args: Array[String]): Unit = {
    val port = if (args.size > 0) args (0).toInt
    else defaultPort

    // Delete the Hive metadata and table data, and the streaming checkpoint directory from previous runs.
    //val fms = new File("metastore_db")
    //val fdy = new File("derby.log")
    deleteHiveMetadata(new File(checkpointDir))
    deleteHiveMetadata(new File(hiveETLDir))

    def processDStream(ssc: StreamingContext, dstream: DStream[String]): StreamingContext = {
      // Create the Hive context and obtain the full path for the HTL dir.
      import spark.implicits._
      import org.apache.spark.sql.functions._

      val hiveETLFile = new java.io.File(hiveETLDir)
      val hiveETLPath = hiveETLFile.getCanonicalPath

      // Create and external table(not managed by Hive itself)
      sqlContext.sql(
        s"""CREATE EXTERNAL TABLE IF NOT EXISTS orders_part(order_id int, order_date string, order_customer_id int)
				PARTITIONED BY(order_status string)
				ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
				LOCATION '$hiveETLPath' """).show()
      println("Tables: ")
      sqlContext.sql("SHOW TABLES").show

      // For each RDD (each DataStream iteration), parse the read text lines into Flight instances, convert to a DataFrame and cache.
      dstream.foreachRDD { rdd =>
        try {
          val orders = rdd
            .map(line => line.split(","))
            .map(r => Orders(r(0).toInt,r(1),r(2).toInt,r(3)))
            .toDF()
            .cache

          orders.show(10)

          // Find the distinct year-month-day combinations and "collect" in a Driver-based Scala-collection
          val uniqueStatus = orders.select("order_status").distinct.collect

          // Loop over the year-month-days. For each one, add a partition with the directory name we want
          uniqueStatus.foreach { row =>
            val status = row.getString(0)
            val partitionPath = "%s/%s".format(hiveETLPath, status)
            sqlContext.sql(
              s"""ALTER TABLE orders_part ADD IF NOT EXISTS PARTITION(order_status = '$status')
							LOCATION '$partitionPath' """)

            // While still looping over order_status, find the corresponding records
            orders
              .where($"order_status" === status) // DON'T write the partition columns.
              .select($"order_id", $"order_date", $"order_customer_id")
              .rdd
              .map(row => row.mkString("|"))
              .saveAsTextFile(partitionPath)
          }
          // Show the partitions we've created
          val showp = sqlContext.sql("SHOW PARTITIONS orders_part")
          val showpCount = showp.count
          println(s"Partitions (${showpCount}):")
          showp.foreach(p => println(" " + p))
        } catch { // Handle failures
          case NonFatal(ex) => sys.exit(1)
        }
      }
      ssc
    }

    def shutdown(ssc: StreamingContext, dataThread: Thread): Unit = {
      println("Shutting down....")
      if (dataThread != null) dataThread.interrupt()
      else println("The dataThread is null!")
      if (ssc != null) ssc.stop(stopSparkContext = true, stopGracefully = true)
      else println("The StreamingContext is null!")
    }

    // It's a convention to use a function to construct the streaming context
    def createContext(): StreamingContext = {
      // Minibatch interval(5 seconds here)
      val ssc = new StreamingContext(sc, interval)
      ssc.checkpoint(checkpointDir)

      // Listen to a socket of text data.
      val dstream = readSocket(ssc, server, port)

      processDStream(ssc, dstream)
    }

    def readSocket(ssc: StreamingContext, server: String, port: Int): DStream[String] = {
      try {
        println(s"Connecting to $server:$port....")
        ssc.socketTextStream(server, port)
      } catch {
        case th: Throwable => {
          ssc.stop()
          throw new RuntimeException(s"Failed to initialize server:port socket with $server:$port...",th)
        }

      }
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
          val inputPath = "/home/cloudera/CCA159DataAnalyst/data/retail_db/orders/part-00000"
          var lineCount = 0
          var passes = 0
          scala.io.Source.fromFile(inputPath).getLines()
            .foreach { line =>
              out.println(line)
              if (lineCount % numRecordsToWritePerBlock == 0) Thread.sleep(pause)
              lineCount += 1
            }
        }finally {
          // Shutdown
          listener.close();
          if (socket != null) socket.close()
        }
      }
    }

    def startSocketDataThread(port: Int): Thread = {
      val dataThread = new Thread(makeRunnable(port))
      // Start the source data thread
      dataThread.start()
      dataThread
    }

    // The Stream event listener
    class EndOfStreamListener(ssc: StreamingContext, dataThread: Thread) extends StreamingListener {
      override def onReceiverError(error: StreamingListenerReceiverError): Unit = {
        println(s"Receiver Error: $error. Stopping....")
        shutdown(ssc, dataThread)
      }

      override def onReceiverStopped(stopped: StreamingListenerReceiverStopped): Unit = {
        println(s"Receiver Stopped: $stopped. Stopping...")
        shutdown(ssc, dataThread)
      }
    }

    // Hack: Use vars and nulls here so we can init these inside the try clause, but see them in the finally clause.
    var ssc: StreamingContext = null
    var dataThread: Thread = null
    try {
      // Start the data source socket in a separate thread
      dataThread = startSocketDataThread(port)

      ssc = StreamingContext.getOrCreate(checkpointDir, createContext _)
      ssc.addStreamingListener(new EndOfStreamListener(ssc, dataThread))
      ssc.start()
      ssc.awaitTerminationOrTimeout(runtime)
    } finally {
      shutdown(ssc, dataThread)
    }

  }
}