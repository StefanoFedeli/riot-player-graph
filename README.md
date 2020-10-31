#RIOT MATCHES GRAPH

## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Setup](#setup)

## General info
This project is created to save insights from League of Legends matches. In particular the process will store in Neo4j server a graph which underline the players who won against an highly banned player. 
	
## Technologies
Riot graph is created with:
* Spark: 2.4.5
* Kafka: last version
* Cassandra: 3.11
* Neo4j Server: 3.5
* Hadoop: 3.1
	
## Setup
To run this project, install it locally:

```
$KAFKA_HOME/bin/zookeeper-server-start.sh
$KAFKA_HOME/config/zookeeper.properties

$KAFKA_HOME/bin/kafka-server-start.sh
$KAFKA_HOME/config/server.properties

$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic matches
```

'''
$HADOOP_HOME/bin/hdfs --daemon start namenode
$HADOOP_HOME/bin/hdfs --daemon start datanode
'''

'''
cassandra -f &
'''

'''
$NEO4J_HOME/bin/neo4j start
'''

***
![Lol logo](https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Ftse1.mm.bing.net%2Fth%3Fid%3DOIP.uD5v6kA6OQvQauF8GHQI4wHaHa%26pid%3DApi&f=1)