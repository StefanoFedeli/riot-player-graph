package scala

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, BooleanType}
import org.neo4j.spark._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.CassandraRow

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
      .config("spark.cassandra.connection.keep_alive_ms","3000")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    val neo = Neo4j(sc)
    val newDB = neo.cypher("MATCH(n) DETACH DELETE n ").loadRowRdd
    println("Neo4J is up and running with " + newDB.count + " nodes")

    val CassRDD: CassandraRDD[CassandraRow] = sc.cassandraTable("riot", "champ")
    //CassRDD.foreach(println)
    val CassTuples: RDD[(Int,String)] = CassRDD.map(pair => (pair.getInt("count"),pair.getString("champion")))
    //CassTuples.foreach(println)
    val smallChamp: Array[String] = CassTuples.filter(c => c._2 != "-1").sortByKey(false).take(Config.CHAMPS).map(el => el._2)

    import sparkSession.implicits._
    val fileEdge : Dataset[(Long,Long,String,String,String)] = sparkSession.read.format("csv").schema(getSchemaEdges()).load(Config.PATH + "edges").as[(Long,Long,String,String,String)]
    val edgesRDD: RDD[Edge[Map[String,String]]] = fileEdge.map(str => new Edge(str._1,str._2, Map("myChampion" -> str._3,
                                                                                              "hisChampion" -> str._4,
                                                                                              "Side" -> str._5))).rdd
    val fileVert : Dataset[(Long,String,Boolean)] = sparkSession.read.format("csv").schema(getSchemaVertex()).load(Config.PATH + "vertexes").as[(Long,String,Boolean)]
    val vertsRDD: RDD[(VertexId,(String,Boolean))] = fileVert.map(str => (str._1,(str._2, str._3))).rdd
    println("RDD Loaded from disk")
    println("--------------------------------------------")


    //create a graph, print some stats
    val graph = Graph.apply(vertsRDD,edgesRDD,("Missing",false))
    println("VERTICI: " + graph.vertices.count)
    println("OF WHICH MISSING: " + graph.vertices.filter{ case (id,(name,track)) => name == "Missing"}.count())
    println("OF WHICH TRACKED: " + graph.vertices.filter{ case (id,(name,track)) => track == true}.count())


    //Our Analysis based on
    println("REAL-TIME DATA STATE THOSE ARE THE MOST BANNED CHAMPIONS:")
    print(smallChamp(0))
    smallChamp.foreach(champ => print(", " + champ))
    println()
    val bannedSubGraph = graph.subgraph(epred = (ed) => ed.attr.get("Side")=="Against").subgraph(epred=ed => smallChamp.contains(ed.attr.get("hisChampion").getOrElse("")) || smallChamp.contains(ed.attr.get("myChampion").getOrElse("")))
    val smallBannedGraph = bannedSubGraph.mapVertices((v_id, v) => v._1)
    val champRelGraph = smallBannedGraph.mapEdges(e => e.attr.get("myChampion").getOrElse("No Champ"))
    //val relGraph = smallBannedGraph.mapEdges(e => e.attr.get("Side").getOrElse("No Info"))
    val trackedGraph = bannedSubGraph.outerJoinVertices(bannedSubGraph.inDegrees) {
                        case (id, u, inDegOpt) =>(u._1, u._2, inDegOpt.getOrElse(0))
                      }.outerJoinVertices(bannedSubGraph.outDegrees) {
                        case (id, u, outDegOpt) => (u._1, u._2, u._3, outDegOpt.getOrElse(0))
                      }
    val numVx = trackedGraph.vertices.filter(vx => vx._2._2 == true).filter{ case (id,vx) => (vx._3 + vx._4) > 0}
    numVx.foreach(println)
    println("Subgraph computation is finished, " + numVx.count() + " tracked players have played against highly banned champions")


    //Save result on Neo4J
    Neo4jGraph.saveGraph(sc,champRelGraph,"name",("BEAT","myChampion"),Some(("USER","id")),Some(("USER","id")),merge=true)
    //Neo4jGraph.saveGraph(sc,relGraph,"name",("SIDE","played"),Some(("USER","id")),Some(("USER","id")),merge=true)
    println("Saved")

    sparkSession.close()
    sc.stop()

  }
}