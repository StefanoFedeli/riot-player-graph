package scala.producer

import scala.collection.mutable.ListBuffer

class playerDataProducerFactory(val API_KEY1: String, val API_KEY2: String, val ENDPOINT_MATCH_LIST_BY_ACCOUNT: String, val ENDPOINT_MATCH_BY_GAME_ID: String, val ENDPOINT_NAME_BY_ACCOUNT: String, val playerList: ListBuffer[String]){
  val producerList = ListBuffer[playerDataProducer]()
  def buildRetrievers(): Unit = {
    for (summonerId: String <- playerList) {
      val newProducer = new playerDataProducer(API_KEY1, API_KEY2, ENDPOINT_MATCH_LIST_BY_ACCOUNT, ENDPOINT_MATCH_BY_GAME_ID, ENDPOINT_NAME_BY_ACCOUNT, summonerId)
      producerList += newProducer
    }
  }
  def queryProducers(): Unit ={
    for (p: playerDataProducer <- producerList){
      p.start()
    }
  }
}