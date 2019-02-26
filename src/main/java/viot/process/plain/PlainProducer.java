package viot.process.plain;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.process.BaseProducer;
import viot.domain.HealthCheck;
import viot.faker.HealthCheckFaker;
import viot.wrapper.ObjectMapperWrapper;

import java.util.Properties;

public final class PlainProducer extends BaseProducer<String, String> {

    public PlainProducer(String brokers) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<>(properties);
    }

    @Override
    public String createValue() {
        HealthCheck healthCheck = HealthCheckFaker.create();
        String json = ObjectMapperWrapper.convert(healthCheck);
        return json;
    }

    public static void main(String[] args) {
        new PlainProducer("localhost:9092").produce(2);
    }

}
