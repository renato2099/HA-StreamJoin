#!/bin/bash

KAFKA_HOME=$1

# remove data folder
rm -r /tmp/kafka-logs

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties