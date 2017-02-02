#!/bin/bash

KAFKA_HOME=$1

# Start zookeeper (assuming it's the default one)
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

# Create topic
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 16 --topic auction-topic