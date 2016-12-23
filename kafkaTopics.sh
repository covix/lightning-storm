#!/bin/bash

PARAM=$1
IFS=',' read -a PARAM_ARRAY <<< "$PARAM"

ZOOKEEPER_URL=cesvima141G3H2:2181

for i in "${PARAM_ARRAY[@]}"
do
        echo "Creating topic", ${i}
        ./kafka-topics.sh --create --zookeeper ${ZOOKEEPER_URL} --replication-factor 1 --partitions 1 --topic ${i}
done

