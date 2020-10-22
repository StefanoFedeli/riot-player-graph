package scala.producer

import java.io.InputStream
import scala.io.Source
import scala.collection.mutable.ListBuffer
import upickle.default._

object Orchestrator {
  def run(): Unit = {

    val stream: InputStream = getClass.getResourceAsStream("/producer_config.json")
    val input = Source.fromInputStream( stream ).getLines.mkString
    val json = ujson.read(input)
    stream.close()

    val API_KEY1 = json("API_KEY1").str
    val API_KEY2 = json("API_KEY2").str
    val ENDPOINT_MATCH_LIST_BY_ACCOUNT = json("ENDPOINT_MATCH_LIST_BY_ACCOUNT").str
    val ENDPOINT_MATCH_BY_GAME_ID = json("ENDPOINT_MATCH_BY_GAME_ID").str

    val playerList = ListBuffer[String]()
    playerList += "rhp-9yRzNcvFJPy-PD1IlL9XvaD-gNKzDsvD5bA1dalxSg"

    val factory = new playerDataProducerFactory(API_KEY1, API_KEY2, ENDPOINT_MATCH_LIST_BY_ACCOUNT, ENDPOINT_MATCH_BY_GAME_ID, playerList)
    /*factory.buildRetrievers()
    println("\nProducers Ready")
    factory.queryProducers()
    println("\nProducers Started")
    */
  }
}
