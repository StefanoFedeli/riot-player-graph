package scala

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, BooleanType}
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
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    import sparkSession.implicits._
    val file : Dataset[(Long,Long,String,String,String,Boolean)] = sparkSession.read.format("csv").option("header", "true").schema(getSchema()).load("hdfs://127.0.0.1:9000/user/stefano/graph-riot").as[(Long,Long,String,String,String,Boolean)]
    file.show()

    val edgesRDD: RDD[Edge[(String,String,String,Boolean)]] = file.map(str => new Edge(str._1,str._2,(str._3,str._4,str._5,str._6) )).rdd
    //val file = Source.fromInputStream(getClass.getResourceAsStream("/edges.csv"))

    /* create edge RDD of type RDD[(VertexId, VertexId)]
    val edgesRDD: RDD[Edge[MatchEdge]] = file.map(line => new Edge() )
      .map(line =>
        (MurmurHash.stringHash(line(0).toString), MurmurHash.stringHash(line(1).toString))
      )
    */
    //create a graph 
    val graph = Graph.fromEdges(edgesRDD, 1)

    // you can see your graph 
    graph.triplets.collect.foreach(println)
  }
}