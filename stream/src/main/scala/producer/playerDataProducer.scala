package scala.producer

import java.io.InputStream
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random
import scala.math.BigInt
import scala.collection.mutable._
import scala.io.Source

class playerDataProducer(val API_KEY1: String, val API_KEY2: String, val ENDPOINT_MATCH_LIST_BY_ACCOUNT: String, val ENDPOINT_MATCH_BY_GAME_ID: String, val ENDPOINT_NAME_BY_ACCOUNT: String, val summonerId: String, val summonerName: String, val champMapping: scala.collection.immutable.Map[String, String], val mode: String) extends Thread{

  var timestamp: Long = 0

  if (mode == "real") {
    timestamp = System.currentTimeMillis - 3600000
  }
  else if (mode == "fake") {
    timestamp = System.currentTimeMillis - (3600000 * 48)
  }

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer_" + summonerId)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "matches"



  override def run(): Unit = {

    while (true) {

      var newTimestamp: Long = 0

      if (mode == "real") {
        newTimestamp = System.currentTimeMillis
      }
      else if (mode == "fake") {
        newTimestamp = timestamp + 8640000
      }

      retrieveGames(timestamp)

      timestamp = newTimestamp

      Thread.sleep(180000)
    }
    producer.close()


  }

  def retrieveGames(beginTime: Long): Unit = {
    var url: String = ""
    if (mode == "real") {
      url = ENDPOINT_MATCH_LIST_BY_ACCOUNT + summonerId + "?queue=420&beginTime=" + beginTime + "&api_key=" + API_KEY2
    }
    else if (mode == "fake") {
      url = ENDPOINT_MATCH_LIST_BY_ACCOUNT + summonerId + "?queue=420&beginTime=" + beginTime + "&endTime=" + (beginTime + 8640000) + "&api_key=" + API_KEY2
    }

    val r = requests.get(url)

    //  Thread.sleep(1200)
    if (r.statusCode < 400) {
      val json = ujson.read(r.text)
      val maxIndex = json("endIndex").num.toInt - 1

      println("Player " + summonerName + " starts retrieving games")
      for (index <- 0 to maxIndex){//maxIndex){
        val game = json("matches")(index)("gameId").num.toLong.toString
        retrieveGameData(game)
        Thread.sleep(11000)
      }
    }
    else {
      println("Player " + summonerName + " has not played any game in the last 3 minutes")
    }
  }

  def retrieveGameData(gameId: String): Unit = {
    val r = requests.get(ENDPOINT_MATCH_BY_GAME_ID + gameId + "?api_key=" + API_KEY1)
    //Thread.sleep(1200)
    if (r.statusCode < 400) {

      val json = ujson.read(r.text)
      val players = json("participantIdentities").arr
      val participants = json("participants").arr
      val duration = json("gameDuration").num
      val ban0 = json("teams")(0)("bans").arr
      val ban1 = json("teams")(1)("bans").arr
      val win0 = json("teams")(0)("win").str
      val win1 = json("teams")(1)("win").str
      val side0 = json("teams")(0)("teamId").num
      val side1 = json("teams")(1)("teamId").num

      var winner = ""
      var winnerSideId = 0
      if (win0 == "Win"){
        if (side0 == 100){
          winner = "Blue"
          winnerSideId = 100
        }
        else {
          winner = "Red"
          winnerSideId = 200
        }
      }
      else{
        if (side1 == 100){
          winner = "Blue"
          winnerSideId = 100
        }
        else {
          winner = "Red"
          winnerSideId = 200
        }
      }

      val bannedChamps = ListBuffer[String]()
      for (c: ujson.Value <- ban0){
        bannedChamps += champMapping.get(c("championId").num.toInt.toString).getOrElse("None")
      }
      for (c: ujson.Value <- ban1){
        bannedChamps += champMapping.get(c("championId").num.toInt.toString).getOrElse("None")
      }

      val edges = ListBuffer[ujson.Obj]()

      var mapping = collection.mutable.Map[String, (String, String)]()
      for (p: ujson.Value <- players) {
        mapping += (p("participantId").num.toInt.toString -> (p("player")("summonerId").str, p("player")("summonerName").str))
      }

      var myParticipantId = ""
      for (p: ujson.Value <- players) {
        if (p("player")("summonerName").str == summonerName) {
          myParticipantId = p("participantId").num.toInt.toString
        }
      }

      var myChampId = ""
      var myTeamId = ""
      for (p: ujson.Value <- participants){
        if (p("participantId").num.toInt.toString == myParticipantId){
          myChampId = p("championId").num.toInt.toString
          myTeamId = p("teamId").num.toInt.toString
        }
      }

      for (p: ujson.Value <- participants){
        if (p("participantId").num.toInt.toString != myParticipantId){

          var outcome = ""
          var competition = ""
          if (p("teamId").num.toInt.toString == myTeamId){
            competition = "Together"
            if (p("teamId").num == winnerSideId){
              outcome = "Win"
            }
            else {
              outcome = "Fail"
            }
          }
          else {
            competition = "Against"
            if (p("teamId").num == winnerSideId){
              outcome = "Win"
            }
            else {
              outcome = "Fail"
            }
          }

          val content: (String, String) = mapping.getOrElse(p("participantId").num.toInt.toString, ("", ""))
          if (content._2 != summonerName) {
            val toAdd = ujson.Obj("hisSummonerId" -> content._1, "hisSummonerName" -> content._2,
                                "myChampionId" -> new ujson.Str(champMapping.get(myChampId).getOrElse("None")), "mySummonerID" -> summonerId, "mySummonerName" -> summonerName,
                                "hisChampionId" -> new ujson.Str(champMapping.get(p("championId").num.toInt.toString).getOrElse("None")),
                                "outcome" -> outcome, "competition" -> competition
            )
            edges += toAdd
          }
          
        }

      }

      val toSubmit = ujson.Obj("gameDuration" -> duration,
                              "winner" -> winner,
                              "bans" -> bannedChamps,
                              "edges" -> edges
                            )

      val data = new ProducerRecord[String, String](TOPIC, summonerName, toSubmit.toString)
      producer.send(data)
    }
    else {
      println("Failed call to: " + ENDPOINT_MATCH_BY_GAME_ID + gameId + "?api_key=" + API_KEY1)
      println(r.text)
    }
  }

  def retrieveGamesData(games: ListBuffer[String]): Unit ={

  }
}
