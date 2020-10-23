$HADOOP_HOME/bin/hdfs --daemon start namenode

$HADOOP_HOME/bin/hdfs --daemon start datanode

cassandra -f &

$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &

$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &

$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic matches

$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic matches --from-beginning &
