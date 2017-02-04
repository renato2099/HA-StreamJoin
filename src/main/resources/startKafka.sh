#!/bin/bash

KAFKA_HOME=$1
NPARTS=$2
# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

# Create topics
 .$KAFKA_HOME/bin/kafka-topics.sh --create --partitions $NPARTS --zookeeper localhost:2181 --replication-factor 1 --topic bid-topic
 .$KAFKA_HOME/bin/kafka-topics.sh --create --partitions $NPARTS --zookeeper localhost:2181 --replication-factor 1 --topic auction-topic
