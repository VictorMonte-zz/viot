#!/usr/bin/env bash

docker exec -it viot_zookeeper_1 kafka-topics --zookeeper localhost:2181 --create --topic healthchecks --replication-factor 1 --partitions 4