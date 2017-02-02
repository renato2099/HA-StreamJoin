#!/bin/bash

KAFKA_HOME=$1
# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
