package scala.producer

import java.io.InputStream
import scala.io.Source
import scala.collection.mutable.ListBuffer
import ujson.Value


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
    val ENDPOINT_NAME_BY_ACCOUNT = json("ENDPOINT_NAME_BY_ACCOUNT").str
    val ENDPOINT_NAME_BY_DIVISION = json("ENDPOINT_NAME_BY_DIVISION").str
    val ENDPOINT_ACCOUNT_BY_NAME = json("ENDPOINT_ACCOUNT_BY_NAME").str

    val playerList: ListBuffer[String] = generatePlayerList(ENDPOINT_NAME_BY_DIVISION, API_KEY1).take(5)
    val playerMap: Map[String, String] = generatePlayerMap(ENDPOINT_ACCOUNT_BY_NAME, API_KEY2, playerList)

    val factory = new playerDataProducerFactory(API_KEY1, API_KEY2, ENDPOINT_MATCH_LIST_BY_ACCOUNT, ENDPOINT_MATCH_BY_GAME_ID, ENDPOINT_NAME_BY_ACCOUNT, playerMap)
    print("DONE! \n Preparing producers...")
    factory.buildRetrievers()
    print("....Producers Ready....")
    factory.queryProducers()
    println("....Producers Started!")

  }

  def generatePlayerList(ENDPOINT_NAME_BY_DIVISION: String, API_KEY1: String): ListBuffer[String] = {
    print("Generating PlayerList...")
    val r = requests.get(ENDPOINT_NAME_BY_DIVISION + "&api_key=" + API_KEY1)
    Thread.sleep(1200)
    val playerList: ListBuffer[String] = ListBuffer[String]()
    if (r.statusCode < 400) {
      val json = ujson.read(r.text)
      for (p: ujson.Value <- json.arr){
        playerList += p("summonerName").str
      }
    }
    println("DONE!")
    return playerList
  }

  def generatePlayerMap(ENDPOINT_ACCOUNT_BY_NAME: String, API_KEY2: String, playerList: ListBuffer[String]): Map[String, String] = {
    println("Generating PlayerMap...")
    var playerMap: Map[String, String] = Map[String, String]()
    var i: Int = 0
    for (p: String <- playerList) {
      println("Querying player " + i.toString + " of " + playerList.size.toString)
      val r = requests.get(ENDPOINT_ACCOUNT_BY_NAME + p + "?api_key=" + API_KEY2)
      Thread.sleep(1200)
      if (r.statusCode < 400) {
        val json = ujson.read(r.text)
        playerMap += (p -> json("accountId").str)
      }
      i += 1
    }
    return playerMap
  }
}
