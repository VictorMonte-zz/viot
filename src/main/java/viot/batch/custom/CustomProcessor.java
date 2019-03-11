package viot.batch.custom;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.batch.BaseProcessor;
import viot.domain.HealthCheck;
import viot.infraestructure.Configuration;
import viot.infraestructure.HealthCheckDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public final class CustomProcessor extends BaseProcessor<KafkaProducer<String, String>, KafkaConsumer<String, HealthCheck>> {

    public CustomProcessor(String brokers) {

        final Properties consumerProperties = getConsumerProperties(brokers, StringDeserializer.class, HealthCheckDeserializer.class);
        consumer = new KafkaConsumer<>(consumerProperties);

        final Properties producerProperties = getProducerProperties(brokers, StringSerializer.class, StringSerializer.class);
        producer = new KafkaProducer<>(producerProperties);
    }

    public void process() {

        consumer.subscribe(Collections.singletonList(Configuration.getHealthCheckTopic()));

        while (true) {

            ConsumerRecords<String, HealthCheck> records = consumer.poll(Duration.ofSeconds(1L));

            for (ConsumerRecord<String, HealthCheck> record : records) {
                HealthCheck healthCheck = record.value();
                sendAndWait(healthCheck);
            }
        }
    }


    public static void main(String[] args) {
        new CustomProcessor("localhost:29092").process();
    }
}


