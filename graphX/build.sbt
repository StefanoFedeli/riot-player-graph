name := "spark_kafka"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" % "spark-graphx_2.11" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.neo4j" % "neo4j-kernel" % "3.5.22",
  "org.neo4j.driver" % "neo4j-java-driver" % "1.7.5",
  "neo4j-contrib" % "neo4j-spark-connector" % "2.4.5-M1"
)