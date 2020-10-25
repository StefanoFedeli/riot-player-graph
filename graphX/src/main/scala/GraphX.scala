package scala

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, BooleanType}
import org.neo4j.spark._

import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}

import scala.io.Source

object graphx {

  def getSchemaEdges() : StructType = {
        return StructType(Seq(StructField("srcID", LongType, true),
                            StructField("dstID", LongType, true),
                            StructField("srcChamp", StringType, true),
                            StructField("dstChamp", StringType, true),
                            StructField("side", StringType, true)
      ))
  }
  def getSchemaVertex() : StructType = {
        return StructType(Seq(StructField("ID", LongType, true),
                            StructField("Name", StringType, true),
                            StructField("Tracking", BooleanType, true)
      ))
  }
  
  def main(args: Array[String]) {

    //create SparkContext
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("GraphFromCSV")
      .config("spark.neo4j.password", "riotintensive")
      .config("spark.neo4j.user", "neo4j")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.keep_alive_ms","60000")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")

    import sparkSession.implicits._
    val fileEdge : Dataset[(Long,Long,String,String,String)] = sparkSession.read.format("csv").schema(getSchemaEdges()).load("hdfs://127.0.0.1:9000/user/dataintensive/graph-riot/edges").as[(Long,Long,String,String,String)]
    val edgesRDD: RDD[Edge[Map[String,String]]] = fileEdge.map(str => new Edge(str._1,str._2, Map("myChampion" -> str._3,
                                                                                              "hisChampion" -> str._4,
                                                                                              "Side" -> str._5))).rdd
    val fileVert : Dataset[(Long,String,Boolean)] = sparkSession.read.format("csv").schema(getSchemaVertex()).load("hdfs://127.0.0.1:9000/user/dataintensive/graph-riot/vertexes").as[(Long,String,Boolean)]
    val vertsRDD: RDD[(VertexId,(String,Boolean))] = fileVert.map(str => (str._1,(str._2, str._3))).rdd
    println("RDD Loaded from disk")
    println("--------------------------------------------")


    //create a graph, print some stats
    val graph = Graph.apply(vertsRDD,edgesRDD,("Missing",false))
    println("VERTICI: " + graph.vertices.count)
    println("DI CUI MANCANTI: " + graph.vertices.filter{ case (id,(name,track)) => name == "Missing"}.count())
    println("DI CUI TRACCIATI: " + graph.vertices.filter{ case (id,(name,track)) => track == true}.count())


    val winsGraph = graph.outerJoinVertices(graph.inDegrees) {
      case (id, old, wins) => (old._1,old._2,wins.getOrElse(0)/5)
    }
    //graph.edges.foreach(item => print(item.attr.get("myChampion").getOrElse("").getClass))

    println(sc.cassandraTable("riot", "champ").as((String, Int)).first())
    //"SELECT champion from riot.champ ORDER BY count DESC LIMIT 5"

    val champs: List[String] = List("350", "80", "360", "40", "236")
    val samiraSubGraph = graph.subgraph(epred = (ed) => champs.contains(ed.attr.get("hisChampion").getOrElse("")))
    println(graph.numEdges)
    println(samiraSubGraph.numEdges)
    //println(subsubgraph.inDegrees.count)
    val toDisplay1 = samiraSubGraph.mapVertices((v_id, v) => v._1)
    val toDisplay2 = toDisplay1.mapEdges(e => e.attr.get("myChampion").getOrElse("No Champ"))



    //Save result on Neo4J
    Neo4jGraph.saveGraph(sc,toDisplay2,"name",("WINNER_WAS_PLAYING","myChampion"),Some(("USER","id")),Some(("USER","id")),merge=true)
    println("Saved")

    sc.stop()
  }
}