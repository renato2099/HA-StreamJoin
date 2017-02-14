#!/usr/bin/env bash

NPARTS=16
MPARTS=0
TUPLES=1000
PCOMPL=0.01
PSUCC=0.5

KAFKA_HOME=/home/marenato/Documents/Apache/Kafka/kafka_2.11-0.10.1.0
JARS=/home/marenato/Documents/workspace/workspacePhd/renatos-and-lukis-information-retrieval-project/HA-StreamJoin

if [ $# -lt 1 ]
then
    echo "<l|h> <mparts>"
    exit 0
fi

if [ -z "$1" ]
then
    KAFKA_HOME=/home/marenato/Documents/Apache/Kafka/kafka_2.11-0.10.1.0
    JARS=/home/marenato/Documents/workspace/workspacePhd/renatos-and-lukis-information-retrieval-project/HA-StreamJoin
else
    KAFKA_HOME=/Users/renatomarroquin/Documents/Apache/Kafka/kafka_2.11-0.10.1.0
    JARS=/Users/renatomarroquin/Documents/workspace/workspacePhd/HA-StreamJoin
fi

if [ -z "$2" ]
then
    MPARTS=0
else
    MPARTS=$2
fi

echo "Lost partitions: $MPARTS"

# Create topics
sh $KAFKA_HOME/bin/kafka-topics.sh --create --partitions $NPARTS --zookeeper localhost:2181 --replication-factor 1 --topic bid-topic
sh $KAFKA_HOME/bin/kafka-topics.sh --create --partitions $NPARTS --zookeeper localhost:2181 --replication-factor 1 --topic auction-topic

echo "Populating <auction-topic>"
echo "kafka=localhost:9092 zk=localhost:2181 missing=${MPARTS} sf=1 tuples=${TUPLES} pcompletion=${PCOMPL} psuccess=${PSUCC}" > auctionProducer.log
java -jar $JARS/target/auction-producer.jar kafka=localhost:9092 zk=localhost:2181 missing=$MPARTS sf=1 tuples=$TUPLES pcompletion=$PCOMPL psuccess=$PSUCC >> auctionProducer.log

echo "Populating <bid-topic>"
echo "kafka=localhost:9092 zk=localhost:2181 missing=${MPARTS} sf=1 tuples=${TUPLES} bid_ratio=10 psuccess=${PSUCC}" > bidProducer.log
java -jar $JARS/target/bid-producer.jar kafka=localhost:9092 zk=localhost:2181 missing=$MPARTS sf=1 tuples=$TUPLES bid_ratio=10 psuccess=$PSUCC >> bidProducer.log

echo "Joining with ${MPARTS} lost"
java -jar $JARS/target/pjoin.jar kafka=localhost:9092 zk=localhost:2181 missing=$MPARTS > join.${MPARTS}.log
echo "# PJOIN #"
cat join.${MPARTS}.log | grep "JoinState:k"

java -jar $JARS/target/hajoin.jar kafka=localhost:9092 zk=localhost:2181 missing=$MPARTS > hajoin.${MPARTS}.log
echo "# HAJOIN #"
cat hajoin.${MPARTS}.log | grep "JoinState:k"