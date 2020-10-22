package scala.producer

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import kafka.producer.KeyedMessage
import ujson.Value
import scala.math.BigInt
import scala.collection.mutable._

class playerDataProducer(val API_KEY1: String, val API_KEY2: String, val ENDPOINT_MATCH_LIST_BY_ACCOUNT: String, val ENDPOINT_MATCH_BY_GAME_ID: String, val summonerId: String) extends Thread{

  //var timestamp: Long = System.currentTimeMillis - 3600000
  var timestamp: Long = 0
  val myID = summonerId


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

      println("NEW BATCH")

      Thread.sleep(150000)
    }
    producer.close()


  }

  def retrieveGames(beginTime: String): Unit = {
    val r = requests.get(ENDPOINT_MATCH_LIST_BY_ACCOUNT + summonerId + "?beginTime=" + beginTime + "&api_key=" + API_KEY1)
    if (r.statusCode < 400) {
      val json = ujson.read(r.text)
      val maxIndex = json("endIndex").num.toInt - 1

      for (index <- 0 to 1){//maxIndex){
        val game = json("matches")(index)("gameId").num.toLong.toString
        retrieveGameData(game)
      }
    }
    else {
      println(r.statusCode)
      println(r.text)
    }
  }

  def retrieveGameData(gameId: String): Unit = {
    val r = requests.get(ENDPOINT_MATCH_BY_GAME_ID + gameId + "?api_key=" + API_KEY2)
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
      var myTeamId = ""
      for (p: ujson.Value <- players) {
        if (p("player")("summonerId").str == summonerId) {
          myParticipantId = p("participantId").num.toInt.toString
          myTeamId = p("teamId").num.toInt.toString

        }
      }

      var myChampId = ""
      for (p: ujson.Value <- participants){
        if (p("participantId").num.toInt.toString != myParticipantId){
          myChampId = p("championId").num.toInt.toString
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
          if (content._1 != myID) {
            val toAdd = ujson.Obj("summonerId" -> content._1, "summonerName" -> content._2,
                                "myChampionId" -> myChampId, "mySummonerID" -> myID,
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

      val data = new ProducerRecord[String, String](TOPIC, summonerId, toSubmit.toString)
      producer.send(data)
    }
    else {
      println(r.statusCode)
      println(r.text)
    }
  }

  def retrieveGamesData(games: ListBuffer[String]): Unit ={

  }
}
