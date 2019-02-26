package viot.process.custom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.process.BaseProducer;
import viot.domain.HealthCheck;
import viot.faker.HealthCheckFaker;
import viot.infraestructure.HealthCheckSerializer;

import java.util.Properties;

public final class CustomProducer extends BaseProducer<String, HealthCheck> {

    public CustomProducer(String brokers) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", HealthCheckSerializer.class);

        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public HealthCheck createValue() {
        return HealthCheckFaker.create();
    }

    public static void main(String[] args) {
        new CustomProducer("localhost:9092").produce(2);
    }
}
