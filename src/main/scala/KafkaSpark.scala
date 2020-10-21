package scala;

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
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

  private def extractMetaData(json: String) : String = {
    return json.split(",")(0)
  }

  private def extractData(json: String) : Long = {
    return json.split(",")(1).toLong
  }

  def main(args: Array[String]) {

    //Creating the SparkStreaming context
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("RiotLOLGraph")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
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

    val metaStream: DStream[String] = kafkaRawStream.map(newRecord => extractMetaData(newRecord.value)).window(Minutes(2))
    metaStream.print()

    val metaStream2m : DStream[String] = metaStream.window(Minutes(2))
    metaStream2m.print()


    val coreStream: DStream[Long] = kafkaRawStream.map(newRecord => extractData(newRecord.value))
    coreStream.print()
    

    /* measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	    <FILL IN>
    }
    val stateDstream = matchesStream.mapWithState(StateSpec.function(mappingFunc)))

    // store the result in Cassandra
    stateDstream.<FILL IN>
    **/

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}
