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

    val link: ListBuffer[MatchEdge] = new ListBuffer[MatchEdge]()
     for (c: ujson.Value <- json("edges").arr){
        link += new MatchEdge(c.toString)
    }
}

@SerialVersionUID(100L)
class MatchEdge(rawJson: String) extends Serializable {

    private var json = ujson.read(rawJson)
    json = if (json.isInstanceOf[Obj]) json else json(0)

    val src: String = json("mySummonerID").str.toString
    val dst: String = json("summonerId").str.toString
    val srcChamp: String = json("myChampionId").str.toString
    val dstChamp: String = json("hisChampionId").str.toString
    val win: Boolean = if(json("hisChampionId").str.toString == "Fail") false else true
    val side: String = json("competition").str.toString
    
    // Overriding tostring method 
    override def toString() : String = { 
          
        return src + " " + dst + " " + json; 
    } 
}

case class championState(championMapping : Map[String,Int]) {
}