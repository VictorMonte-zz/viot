# Viot

Java app for IoT Streaming

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

