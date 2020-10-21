package scala;

import scala.producer.Orchestrator

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

import scala.collection.immutable.HashMap

import com.datastax.driver.core.{Session, Cluster}
//import java.util.{Date, Properties}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

/*
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
*/

object KafkaSpark {

  def mappingFunc(key: Int, value: Option[Match], state: State[championState]): Map[String,Int] = {
    //Update Map of champions
    val champs : championState = state.getOption.getOrElse(championState(new Map[String,Int]()))
    val chall = value.getOrElse(null)
    var finalMap: Map[String,Int] = new Map[String,Int]() 
    if (chall != null) {
      for (id: String <- chall.banList) {
        var num = champs.championMapping.get(id)
        finalMap = champs.championMapping + (id -> (num+1))
      }
    }
    
    print(champs.championMapping)
    var out = new Map[String,Int]()
    return out
  }

  def main(args: Array[String]) {

    //Creating the SparkStreaming context
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("RiotLOLGraph")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    val sparkStreamingContext = new StreamingContext(sparkSession.sparkContext, Minutes(1))

    Orchestrator.run()

    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS riot WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1};")
    session.execute("CREATE TABLE IF NOT EXISTS riot.stats ( slot timestamp PRIMARY KEY, duration float, red_win int, tot_matches int)")
    session.execute("CREATE COLUMNFAMILY IF NOT EXISTS riot.stats ( champion text PRIMARY KEY, count bigint)")

    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConfig = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
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

    val metaStream: DStream[Map[String,Int]] = kafkaRawStream.map(newRecord => (1,new Match(newRecord.value))).mapWithState(StateSpec.function(mappingFunc _))
    metaStream.print()

    //val metaStream2m : DStream[String] = metaStream.window(Minutes(2))
    //metaStream2m.print()


    //val coreStream: DStream[Long] = kafkaRawStream.map(newRecord => newRecord.value)
    //coreStream.print()
    

    /* measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[Double]): (String, Double) = {
	    <FILL IN>
    }
    val stateDstream = matchesStream.mapWithState(StateSpec.function(mappingFunc)))

    // store the result in Cassandra
    stateDstream.<FILL IN>
    **/

    sparkStreamingContext.checkpoint("./checkpoints")
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}
