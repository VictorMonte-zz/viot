package viot.process.custom;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import viot.domain.HealthCheck;
import viot.process.BaseConsumer;
import viot.infraestructure.HealthCheckDeserializer;

import java.util.Properties;

public class CustomConsumer extends BaseConsumer<String, HealthCheck> {

    public CustomConsumer(String brokers) {
        Properties properties = new Properties();
        properties.put("group.id", "healthcheck-processor");
        properties.put("bootstrap.servers", brokers);
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer", HealthCheckDeserializer.class);
        consumer = new KafkaConsumer<>(properties);
    }
}
