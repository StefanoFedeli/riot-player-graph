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
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.graphx.Edge
import org.apache.spark.streaming._

import scala.collection.immutable.HashMap

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
//import java.util.{Date, Properties}
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

/*
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
*/

import scala.collection.mutable._

object KafkaSpark {

  var systime: Long = System.currentTimeMillis

  def getSystemTime(reset: Boolean) : Long = {
    if (reset) {
      systime = System.currentTimeMillis
    }
    return systime
  }

  def mappingFunc(key: String, value: Option[Int], state: State[HashMap[String, Int]]): (String, Int) = {
    val oldState = state.getOption.getOrElse(new HashMap[String, Int]())
    var newState = oldState
    val oldValue: Int = oldState.getOrElse(key, 0)
    newState += key -> (oldValue + value.getOrElse(0))
    state.update(newState)
    return (key, oldValue + value.getOrElse(0))
  }

  def main(args: Array[String]) {

    //Creating the SparkStreaming context
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("RiotLOLGraph")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()
    //val sparkStreamingContext = new StreamingContext(sparkSession.sparkContext, Minutes(1))
    val sparkStreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(30))

    //Run the Kafka producers
    Orchestrator.run()

    // connect to Cassandra and make a keyspace and tables
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS riot WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1};")
    session.execute("CREATE TABLE IF NOT EXISTS riot.stats ( slot timestamp PRIMARY KEY, duration float, red_win int, tot_matches int);")
    session.execute("CREATE TABLE IF NOT EXISTS riot.champ ( champion text PRIMARY KEY, count bigint);")

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

    //Save the data to Cassandra
    val matchList: DStream[Match] = kafkaRawStream.map(newRecord => new Match(newRecord.value))
    
    val metaStream: DStream[(String, Int)] = matchList.map(m => m.banList).flatMap(e => e).map(champ => (champ, 1)).mapWithState(StateSpec.function(mappingFunc _))
    metaStream.saveToCassandra("riot", "champ", SomeColumns("champion", "count"))

    val matchesAgg: DStream[Match] = matchList.window(Minutes(5))
    val durationAvg: DStream[(Long, Float)] = matchesAgg.map(m => m.duration).reduce((x,y) => x + y).map(tot => (getSystemTime(true),tot))
    val winRedTeam: DStream[(Long, Long)] = matchesAgg.filter(m => m.winTeam == "Red").count().map(tot => (getSystemTime(false),tot))
    val totalMatches: DStream[(Long, Long)] = matchesAgg.count().map(tot => (getSystemTime(false),tot))
    durationAvg.saveToCassandra("riot","stats",SomeColumns("slot", "duration"))
    winRedTeam.saveToCassandra("riot","stats",SomeColumns("slot", "red_win"))
    totalMatches.saveToCassandra("riot","stats",SomeColumns("slot", "tot_matches"))


    //Get Edges
    val edgeList: DStream[(Long,Long,String,String,String,Boolean)] = matchList.map(m => m.link).flatMap(e => e).map(edge => edge.toTuple)
    edgeList.print()

    //Append edgeList in hdfs
    edgeList.foreachRDD(rdd => {
      rdd.foreach(println)
      if (!rdd.isEmpty()) {
        import sparkSession.implicits._
        sparkSession.createDataset(rdd).write.format("csv").mode("append").save("hdfs://127.0.0.1:9000/user/dataintensive/graph-riot/")
      }
    })

    // Start the Spark Job
    sparkStreamingContext.checkpoint("./checkpoints")
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}
