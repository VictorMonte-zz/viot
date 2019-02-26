package viot.process;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BaseConsumer<K, V> {

    protected KafkaConsumer<K, V> consumer;

}
