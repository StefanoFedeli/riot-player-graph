package scala

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.util.hashing.MurmurHash3

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

    private val json = ujson.read(rawJson)
    val srcName: String =  json("mySummonerName").str.toString
    val dstName: String =  json("hisSummonerName").str.toString
    val src: Long = MurmurHash3.stringHash(srcName)
    val dst: Long = MurmurHash3.stringHash(dstName)
    val srcChamp: String = json("myChampionId").str.toString
    val dstChamp: String = json("hisChampionId").str.toString
    val win: Boolean = if(json("outcome").str.toString == "Fail") false else true
    val side: String = json("competition").str.toString
    
    // Overriding tostring method 
    override def toString() : String = {
        return src + "," + dst + "," + srcChamp + "," + dstChamp + "," + win + "," + side; 
    }

    def toTuple() : (Long,Long,String,String,String) = {
        if (win) {
            return (src,dst,srcChamp,dstChamp,side) 
        } else {
            return (dst,src,srcChamp,dstChamp,side) 
        }
    }

    def extractVertex() : ListBuffer[(Long,String,Boolean)] = {
        var vertex: ListBuffer[(Long,String,Boolean)] = ListBuffer[(Long,String,Boolean)]()
        
        vertex.append((src,srcName,true))
        vertex.append((dst,dstName,false))
        return vertex
    }
    
}