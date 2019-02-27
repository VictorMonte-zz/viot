package viot.process.custom;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.domain.HealthCheck;
import viot.infraestructure.Configuration;
import viot.infraestructure.HealthCheckDeserializer;
import viot.service.MonitorService;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class CustomProcessor {

    private final KafkaConsumer<String, HealthCheck> consumer;
    private final KafkaProducer<String, String> producer;

    public CustomProcessor(String brokers) {

        final Properties consumerProperties = new Properties();
        consumerProperties.put("group.id", "healthcheck-processor");
        consumerProperties.put("bootstrap.servers", brokers);
        consumerProperties.put("key.deserializer", StringDeserializer.class);
        consumerProperties.put("value.deserializer", HealthCheckDeserializer.class);
        consumer = new KafkaConsumer<>(consumerProperties);

        final Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", brokers);
        producerProperties.put("key.serializer", StringSerializer.class);
        producerProperties.put("value.serializer", StringSerializer.class);
        producer = new KafkaProducer<>(producerProperties);
    }

    public void process() {

        final MonitorService monitor = new MonitorService();
        consumer.subscribe(Collections.singletonList(Configuration.getHealthCheckTopic()));

        while (true) {

            ConsumerRecords<String, HealthCheck> records = consumer.poll(Duration.ofSeconds(1L));

            for (ConsumerRecord<String, HealthCheck> record : records) {

                HealthCheck healthCheck = record.value();

                if (hasHealthCheck(healthCheck)) {

                    int uptime = monitor.getUptime(healthCheck);

                    Future<RecordMetadata> future = producer.send(
                            new ProducerRecord<>(
                                    Configuration.getUptimesTopic(),
                                    healthCheck.getSerialNumber(),
                                    String.valueOf(uptime))
                    );

                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private boolean hasHealthCheck(HealthCheck healthCheck) {
        return healthCheck != null;
    }

    public static void main(String[] args) {
        new CustomProcessor("localhost:9092").process();
    }
}


