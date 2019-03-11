# Viot

Java app for IoT Streaming.
Viot is a app to simulate streaming of weather conditions
from IoT gadgets. Then it calculates the uptime and publish it.

Payload
```
{  
   "event":"HEALTH_CHECK",
   "factory":"Micheleton",
   "serialNumber":"ZL71-BV45",
   "type":"SOLAR",
   "status":"RUNNING",
   "lastStartedAt":"2018-12-17T02:31:59.668+0000",
   "temperature":68.0,
   "ipAddress":"175.245.169.59"
}
```

## Getting started

### Build and Running

```
docker-compose up --build
```

### Configuring

Inside zookeeper instance

```
docker exec -it viot_zookeeper_1 kafka-topics --zookeeper localhost:32181 --create --topic healthchecks --replication-factor 1 --partitions 4
```

```
docker exec -it viot_zookeeper_1 kafka-topics --zookeeper localhost:32181 --create --topic uptimes --replication-factor 1 --partitions 4
```

```
docker exec -it viot_zookeeper_1 kafka-topics --zookeeper localhost:32181 --create --topic healthchecks-avro --replication-factor 1 --partitions 4
```

Add your ip to your hosts(For Mac) Ref: https://github.com/santthosh/kafka-for-mac

```
ifconfig | grep -e "inet "
sudo vim /etc/hosts
```

Creating avro schema

```
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
--data '{ "schema": "{ \"name\": \"HealthCheck\", \"namespace\": \"viot.avro\", \"type\": \"record\", \"fields\": [ { \"name\": \"event\", \"type\": \"string\" }, { \"name\": \"factory\", \"type\": \"string\" }, { \"name\": \"serialNumber\", \"type\": \"string\" }, { \"name\": \"type\", \"type\": \"string\" }, { \"name\": \"status\", \"type\": \"string\"}, { \"name\": \"lastStartedAt\", \"type\": \"long\", \"logicalType\": \"timestamp-millis\"}, { \"name\": \"temperature\", \"type\": \"float\" }, { \"name\": \"ipAddress\", \"type\": \"string\" } ]} " }' \
http://localhost:29081/subjects/healthchecks-avro-value/versions
```

## Testing

Consuming to check data from Kakfa

```
docker exec -it viot_kafka_1 kafka-console-consumer --bootstrap-server localhost:29092 --topic healthchecks
```

```
docker exec -it viot_kafka_1 kafka-console-consumer --bootstrap-server localhost:29092 --topic uptimes --property print.key=true
```

```
docker exec -it viot_kafka_1 kafka-console-consumer --bootstrap-server localhost:29092 --topic healthchecks-avro
```

Running app Viot APP local

Producing:
```
java -cp ./build/libs/viot-0.1.0.jar viot.batch.plain.PlainProducer
```

Consuming:
```
java -cp ./build/libs/viot-0.1.0.jar viot.batch.plain.PlainStreamsProcessor
```