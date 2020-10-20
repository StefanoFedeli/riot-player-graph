name := "spark_kafka"

version := "1.0"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" % "spark-streaming_2.12" % "2.4.5",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "2.4.5",
  "org.apache.spark" % "spark-graphx_2.12" % "2.4.5",
  //("com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2").exclude("io.netty", "netty-handler"),
  //("com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0").exclude("io.netty", "netty-handler")
  "com.lihaoyi" %% "requests" % "0.6.5"
)

