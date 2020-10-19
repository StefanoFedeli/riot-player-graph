package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.SparkConf
import org.apache.spark.graphx.Edge
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

/*
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
*/

object KafkaSpark {
  def main(args: Array[String]) {

    //Creating the SparkStreaming context
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("RiotLOLGraph")
    val sparkStreamingContext = new StreamingContext(sparkConfig, Minutes(5))

    /** connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.<FILL IN>
    val session = cluster.connect()
    session.execute(.<FILL IN>)
    **/

    // make a connection to Kafka and read (key, value) pairs from it
     val kafkaConfig = Map[String, Object](
      //"client.dns.lookup" -> "resolve_canonical_bootstrap_servers_only",
      "bootstrap.servers" -> "locahost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafkaSparkTestGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kafkaTopics = Array("matches")

    val kafkaRawStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      sparkConfig,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](kafkaTopics, kafkaConfig)
    )

    val matchesStream: DStream[Edge] = kafkaRawStream.map (newRecord => new Edge(newRecord.key, newRecord.value, 1))

    val recordsCount: DStream[Long] = matchesStream.count()

    /** measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	    <FILL IN>
    }
    val stateDstream = pairs.mapWithState(<FILL IN>)

    // store the result in Cassandra
    stateDstream.<FILL IN>
    **/
    println(recordsCount)

    ssc.start()
    ssc.awaitTermination()
  }
}
