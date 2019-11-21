package spark_streaming

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

/**
  * streaming request count
  * create a kafka topic called gen_logs
  * $ kafka-topics --create --zookeeper quickstart.cloudera:2181 --replication-factor 1 --topic gen_logs
  * $ kafka-topics --list --zookeeper quickstart.cloudera:2181
  * $ start_logs
  * create or Run a Producer
  * $ tail_logs | kafka-console-producer --broker-list quickstart.cloudera:9092 --topic gen_logs
  * Run DirectKafkaIntegration
  */
object DirectKafkaIntegration {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val envProps: Config = conf.getConfig(args(0))

    val spark = SparkSession
      .builder()
      .appName("Direct Kafka Integration")
      .master(envProps.getString("execution.mode"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ssc = new StreamingContext(sc, Seconds(envProps.getInt("window")))

    val topicSet = Set(envProps.getString("topic"))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> envProps.getString("bootstrap.server"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaStream: DStream[String] = KafkaUtils
      .createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topicSet, kafkaParams))
      .map(record => record.value)

    val userreqs = kafkaStream
        .map(line => (line.split(' ')(0), 1))
        .reduceByKey( (x, y) => x + y)

    userreqs.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
