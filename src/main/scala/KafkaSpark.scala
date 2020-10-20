package scala;

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.graphx.Edge
import org.apache.spark.streaming._
//import java.util.{Date, Properties}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

/*
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
*/

object KafkaSpark {
  def main(args: Array[String]) {

    //Creating the SparkStreaming context
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("RiotLOLGraph")
      .getOrCreate()

    val sparkStreamingContext = new StreamingContext(sparkSession.sparkContext, Minutes(1))

    /** connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.<FILL IN>
    val session = cluster.connect()
    session.execute(.<FILL IN>)
    **/

    // make a connection to Kafka and read (key, value) pairs from it
     val kafkaConfig = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka-spark-streaming"
    )
    val kafkaTopics = Array("matches")

    val kafkaRawStream = KafkaUtils.createDirectStream[String, String](
      sparkStreamingContext,
      PreferConsistent,
      Subscribe[String, String](kafkaTopics, kafkaConfig)
    )

    val vertex = sparkSession.read.csv("hdfs://127.0.0.1:9000/user/stefano/graph-riot/vertex.csv")
    val edges = sparkSession.read.csv("hdfs://127.0.0.1:9000/user/stefano/graph-riot/edges.csv")

    val matchesStream: DStream[String] = kafkaRawStream.map(newRecord => newRecord.key)
    val recordsCount: DStream[Long] = matchesStream.count()

    /** measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	    <FILL IN>
    }
    val stateDstream = pairs.mapWithState(<FILL IN>)

    // store the result in Cassandra
    stateDstream.<FILL IN>
    **/
    recordsCount.print()

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}
