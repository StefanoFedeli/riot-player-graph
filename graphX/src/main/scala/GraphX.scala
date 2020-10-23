package scala

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, BooleanType}
import org.neo4j.spark._

import scala.io.Source

object graphx {

  def getSchema() : StructType = {
        return StructType(Seq(StructField("srcID", LongType, true),
                            StructField("dstID", LongType, true),
                            StructField("srcChamp", StringType, true),
                            StructField("dstChamp", StringType, true),
                            StructField("side", StringType, true),
                            StructField("win", BooleanType, true)
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
      .getOrCreate()
    val sc = sparkSession.sparkContext

    import sparkSession.implicits._
    val file : Dataset[(Long,Long,String,String,String,Boolean)] = sparkSession.read.format("csv").schema(getSchema()).load("hdfs://127.0.0.1:9000/user/dataintensive/graph-riot").as[(Long,Long,String,String,String,Boolean)]

    val edgesRDD: RDD[Edge[(String,String,String,Boolean)]] = file.map(str => new Edge(str._1,str._2,(str._3,str._4,str._5,str._6) )).rdd

    //create a graph 
    val graph = Graph.fromEdges(edgesRDD, 1)
    val subgraph = graph.mapEdges(ed => ed.attr._2)
    for (triplet <- subgraph.triplets.collect) {
      println(s"${triplet.srcAttr} likes ${triplet.dstAttr}")
    }
    //val neo = Neo4j(sc)
    //neo.saveGraph(graph, "matches")
    println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")
    Neo4jGraph.saveGraph(sc,subgraph,"rank",("CHALLENGE","data"),Some(("USER","id")),Some(("USER","id")),merge=true)
    println("Saved")
    println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO")

    sc.stop()
  }
}