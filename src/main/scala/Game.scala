package scala

import scala.collection.mutable.ListBuffer
import ujson.Obj

class Player {
    
}

@SerialVersionUID(100L)
class Match (rawJson : String) extends Serializable {
    
    private var json = ujson.read(rawJson)
    json = if (json.isInstanceOf[Obj]) json else json(0)

    val duration: Long = json("gameDuration").num.toLong
    val winTeam: String = json("winner").str.toString
    val banList : ListBuffer[String] = ListBuffer[String]()
    for (c: ujson.Value <- json("bans").arr){
        banList += c.str
    }
}

class MatchEdge {

}

case class championState(championMapping : Map[String,Int]) {
}