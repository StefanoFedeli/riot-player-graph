package main.scala.producer

import java.io.FileInputStream
import scala.collection.mutable.ListBuffer
import upickle.default._

object orchestrator {
  def main(args: Array[String]): Unit = {
    val file = scala.io.Source.fromFile("src/main/scala/producer/config.json")
    val input = try file.mkString finally file.close()
    val json = ujson.read(input)

    val API_KEY1 = json("API_KEY1").str
    val API_KEY2 = json("API_KEY2").str
    val ENDPOINT_MATCH_LIST_BY_ACCOUNT = json("ENDPOINT_MATCH_LIST_BY_ACCOUNT").str
    val ENDPOINT_MATCH_BY_GAME_ID = json("ENDPOINT_MATCH_BY_GAME_ID").str

    val playerList = ListBuffer[String]()
    playerList += "rhp-9yRzNcvFJPy-PD1IlL9XvaD-gNKzDsvD5bA1dalxSg"

    val factory = new playerDataProducerFactory(API_KEY1, API_KEY2, ENDPOINT_MATCH_LIST_BY_ACCOUNT, ENDPOINT_MATCH_BY_GAME_ID, playerList)
    factory.buildRetrievers()
    factory.queryProducers()
  }
}
