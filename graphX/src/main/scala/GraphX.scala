package scala

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash
import scala.io.Source

object graphx {
  
  def main(args: Array[String]) {

    //create SparkContext
    val sparkConf = new SparkConf().setAppName("GraphFromCSV").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val file = Source.fromInputStream(getClass.getResourceAsStream("/edges.csv"))

    // create edge RDD of type RDD[(VertexId, VertexId)]
    val edgesRDD: RDD[Edge[MatchEdge]] = file.map(line => new Edge() )
      .map(line =>
        (MurmurHash.stringHash(line(0).toString), MurmurHash.stringHash(line(1).toString))
      )

    // create a graph 
    val graph = Graph.fromEdgeTuples(edgesRDD, 1)

    // you can see your graph 
    graph.triplets.collect.foreach(println)

  }
}