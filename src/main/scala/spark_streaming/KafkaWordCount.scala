package spark_streaming

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


/**
	* Create kafka topic
	* $ kafka-topics --zookeeper quickstart.cloudera:2181 --create --topic spark_streaming --partitions 3 --replication-factor 1
	*
	* Start logs
	* $ start_logs
	*
	* Put logs into kafka
	* $ tail_logs |  kafka-console-producer --broker-list quickstart.cloudera:9092 --topic spark_streaming
	*
	* run KafkaWordCount.scala
	*
	* -------------------------------------------
	* Time: 1567543710000 ms
	* -------------------------------------------
	* (253,1)
	* (202,1)
	* (KHTML,10)
	* (82,1)
	* (96,1)
	* (232,1)
	* (19,1)
	* (1985,3)
	* (0800,10)
	* (992,1)
	*/
object KafkaWordCount {
	def main(args: Array[String]): Unit = {
		val conf = ConfigFactory.load
		val envProps: Config = conf.getConfig(args(0))

		val sparkConf = new SparkConf().setMaster(envProps.getString("execution.mode")).setAppName("KafkaReceiver")
		sparkConf.set("spark.testing.memory", "2147480000")
		val streamingContext = new StreamingContext(sparkConf, Seconds(envProps.getInt("window")))
		streamingContext.sparkContext.setLogLevel("INFO")

		val topicsSet = Set(envProps.getString("topic"))
		val kafkaParams = Map[String, Object] (
			"bootstrap.servers" -> envProps.getString("bootstrap.server"),
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "1",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)
		val logData: DStream[String] = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent,Subscribe[String,String](topicsSet, kafkaParams))
  			.map(record => record.value)

		val words = logData.flatMap(line => line.split("\\W"))
		val wordsCounts = words.map(w => (w, 1)).reduceByKey( (v, v1) => v + v1)
		wordsCounts.print() //Prints the wordcount result of the stream

		streamingContext.start()
		streamingContext.awaitTermination()
	}
}
