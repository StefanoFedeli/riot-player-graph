package producer

import scala.collection.mutable.ListBuffer
import producer.playerDataProducer

object playerDataProducerFactory(val playerList: List[String]){
  def buildRetrievers(): List[playerDataProducer] = {
    var retrieversFactory = new ListBuffer[String]()
    for summonerId: String in playerList:
    retrieversFactory += new playerDataProducer(summonerId)
    return retrieversFactory
  }:

}