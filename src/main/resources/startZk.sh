#!/bin/bash

KAFKA_HOME=$1

# remove data folder
rm -r /tmp/zookeeper
# Start zookeeper (assuming it's the default one)
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
