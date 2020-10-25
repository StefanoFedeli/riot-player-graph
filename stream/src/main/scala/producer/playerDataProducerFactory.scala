package scala.producer

import scala.collection.mutable.ListBuffer
import scala.io.Source

class playerDataProducerFactory(val API_KEY1: String, val API_KEY2: String, val ENDPOINT_MATCH_LIST_BY_ACCOUNT: String, val ENDPOINT_MATCH_BY_GAME_ID: String, val ENDPOINT_NAME_BY_ACCOUNT: String, val playerMap: Map[String, String]){
  val producerList = ListBuffer[playerDataProducer]()
  def buildRetrievers(): Unit = {

    val lines = Source.fromInputStream(getClass.getResourceAsStream("/champs.txt")).getLines.toList
    var champMapping: Map[String, String] = Map[String, String]()
    for (l: String <- lines){
      val splitted: Array[String] = l.split(",")
      champMapping += (splitted(0) -> splitted(1))
    }

    for ((summonerName, summonerId) <- playerMap) {
      val newProducer = new playerDataProducer(API_KEY1, API_KEY2, ENDPOINT_MATCH_LIST_BY_ACCOUNT, ENDPOINT_MATCH_BY_GAME_ID, ENDPOINT_NAME_BY_ACCOUNT, summonerId, summonerName, champMapping)
      producerList += newProducer
    }
  }
  def queryProducers(): Unit ={
    for (p: playerDataProducer <- producerList){
      p.start()
    }
  }
}