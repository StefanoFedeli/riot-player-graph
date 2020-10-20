package producer

import java.util.Properties

class playerDataProducer(val API_KEY: String, val ENDPOINT_MATCH_LIST_BY_ACCOUNT: String, val ENDPOINT_MATCH_BY_GAME_ID, val summonerId: String) extends App {

  /*
  val props:Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhosts:9092")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "PlayerStreamProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "matches"
  */

  def retrieveGames(beginTime: String) = {
    val r = requests.get(ENDPOINT_MATCH_LIST_BY_ACCOUNT + summonerId + "?beginTime=" + beginTime + "&api_key=" + API_KEY)
    print(r.text)
  }

  def retrieveGameData(gameId: String) = {
    val r = requests.get(ENDPOINT_MATCH_BY_GAME_ID + gameId + "?api_key=" + API_KEY)
    print(r.text)
  }
}