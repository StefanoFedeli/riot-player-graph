name := "spark_kafka"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" % "spark-streaming_2.11" % "2.4.5",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.4.5",
  "org.apache.spark" % "spark-graphx_2.11" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.kafka" % "kafka_2.11" % "1.0.0",
  "com.lihaoyi" %% "requests" % "0.1.9",
  "com.lihaoyi" %% "upickle" % "0.7.4",
  //("com.datastax.cassandra" % "cassandra-driver-core" % "3.6.0").exclude("io.netty", "netty-handler")
  ("com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2").exclude("io.netty", "netty-handler"),
)

dependencyOverrides ++= {
  Seq(
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.1",
  )
}