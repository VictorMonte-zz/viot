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
kafka-topics --zookeeper localhost:2181 --create --topic healthchecks --replication-factor 1 --partitions 4
```

```
kafka-topics --zookeeper localhost:2181 --create --topic uptimes --replication-factor 1 --partitions 4
```

## Testing

Inside kafka

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic healthchecks
```

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic uptimes --property print.key=true
```

