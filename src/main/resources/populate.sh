#!/usr/bin/env bash

#KAFKA_HOME=/home/marenato/Documents/Apache/Kafka/kafka_2.11-0.10.1.0
KAFKA_HOME=/Users/renatomarroquin/Documents/Apache/Kafka/kafka_2.11-0.10.1.0
#JARS=/home/marenato/Documents/workspace/workspacePhd/renatos-and-lukis-information-retrieval-project/HA-StreamJoin
JARS=/Users/renatomarroquin/Documents/workspace/workspacePhd/HA-StreamJoin
NPARTS=16

# Create topics
sh $KAFKA_HOME/bin/kafka-topics.sh --create --partitions $NPARTS --zookeeper localhost:2181 --replication-factor 1 --topic bid-topic
sh $KAFKA_HOME/bin/kafka-topics.sh --create --partitions $NPARTS --zookeeper localhost:2181 --replication-factor 1 --topic auction-topic

java -jar $JARS/target/bid-producer.jar kafka=localhost:9092 zk=localhost:2181 missing=0 sf=1 tuples=10 bid_ratio=2
java -jar $JARS/target/auction-producer.jar kafka=localhost:9092 zk=localhost:2181 missing=0 sf=1 tuples=10 pcompletion=1.0