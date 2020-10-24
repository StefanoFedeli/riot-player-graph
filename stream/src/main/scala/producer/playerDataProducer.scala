package scala.producer

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage
import ujson.Value
import scala.math.BigInt
import scala.collection.mutable._

class playerDataProducer(val API_KEY1: String, val API_KEY2: String, val ENDPOINT_MATCH_LIST_BY_ACCOUNT: String, val ENDPOINT_MATCH_BY_GAME_ID: String, val ENDPOINT_NAME_BY_ACCOUNT: String, val summonerId: String, val summonerName: String) extends Thread{

  //var timestamp: Long = System.currentTimeMillis - 3600000
  var timestamp: Long = 0

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer_" + summonerId)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "matches"

  override def run(): Unit = {

    while (true) {

      val newTimeStamp = System.currentTimeMillis

      retrieveGames(timestamp.toString)

      timestamp = newTimeStamp

      Thread.sleep(150000)
    }
    producer.close()


  }

  def retrieveGames(beginTime: String): Unit = {
    val r = requests.get(ENDPOINT_MATCH_LIST_BY_ACCOUNT + summonerId + "?queue=420&beginTime=" + beginTime + "&api_key=" + API_KEY2)
    Thread.sleep(1200)
    if (r.statusCode < 400) {
      val json = ujson.read(r.text)
      val maxIndex = json("endIndex").num.toInt - 1

      println("Player " + summonerName + " starts retriving 5 Games //TODO FIX IT FOR PRODUCTION")
      for (index <- 0 to 5){//maxIndex){
        val game = json("matches")(index)("gameId").num.toLong.toString
        retrieveGameData(game)
        Thread.sleep(11000)
      }
    }
    else {
      println("ERRRRRROR")
      println(r.statusCode)
      println(r.text)
    }
  }

  def retrieveGameData(gameId: String): Unit = {
    val r = requests.get(ENDPOINT_MATCH_BY_GAME_ID + gameId + "?api_key=" + API_KEY1)
    Thread.sleep(1200)
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
        bannedChamps += c("championId").num.toInt.toString
      }
      for (c: ujson.Value <- ban1){
        bannedChamps += c("championId").num.toInt.toString
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
                                "myChampionId" -> myChampId, "mySummonerID" -> summonerId, "mySummonerName" -> summonerName,
                                "hisChampionId" -> p("championId").num.toInt.toString,
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
    }
  }

  def retrieveGamesData(games: ListBuffer[String]): Unit ={

  }
}
