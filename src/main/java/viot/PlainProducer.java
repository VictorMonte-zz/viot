package viot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import viot.domain.HealthCheck;
import viot.faker.HealthCheckFaker;
import viot.helper.JsonConverter;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class PlainProducer {

    private final KafkaProducer<String, String> producer;
    private static final Logger logger = LogManager.getLogger(PlainProducer.class.getName());

    public PlainProducer(String brokers) {
        Properties properties = getProperties(brokers);
        producer = new KafkaProducer<>(properties);
    }

    private Properties getProperties(String brokers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", StringSerializer.class);
        return properties;
    }

    public void produce(int ratesPerSecond) {

        while (true) {

            HealthCheck healthCheck = HealthCheckFaker.create();
            String healthCheckJson = JsonConverter.convert(healthCheck);

            logger.info("Sending health check message");

            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>("healthchecks", healthCheckJson)
            );

            try {

                long millis = 1000L / ratesPerSecond;
                Thread.sleep(millis);
                future.get();

                logger.info("Message sent with success");

            } catch (InterruptedException | ExecutionException e) {
                logger.error(e);
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        new PlainProducer("localhost:9092").produce(2);
    }
}
