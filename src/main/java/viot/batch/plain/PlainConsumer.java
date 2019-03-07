package viot.batch.plain;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import viot.batch.BaseConsumer;

import java.util.Properties;

public final class PlainConsumer extends BaseConsumer<String, String> {

    public PlainConsumer(String brokers) {
        Properties properties = new Properties();
        properties.put("group.id", "healthcheck-processor");
        properties.put("bootstrap.servers", brokers);
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", StringDeserializer.class);
        consumer = new KafkaConsumer<>(properties);
    }
}
