package generator

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import kafka.producer.KeyedMessage

object PlayerStreamProducer extends App {

    val API_KEY = "RGAPI-1d838c0e-62ad-4363-a84f-9f9271668ab1" //Expires: Sat, Oct 17th, 2020 @ 10:24am (PT)
    val sampleId = "rhp-9yRzNcvFJPy-PD1IlL9XvaD-gNKzDsvD5bA1dalxSg"
    val ENDPOINT_LIST = "https://euw1.api.riotgames.com/lol/match/v4/matchlists/by-account/"
    val ENDPOINT_MATCHES = "https://euw1.api.riotgames.com/lol/match/v4/matches/"
    val TOPIC = "matches"

    private val SummonerID = ""


    
    val props:Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhosts:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "PlayerStreamProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    while (true) {
        
        val data = new ProducerRecord[String, String](TOPIC, SummonerID , "3e" )
        producer.send(data)
        print(data + "\n")
    }

    producer.close()
}
