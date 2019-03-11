package viot.batch;

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

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class BaseProcessor<Producer extends KafkaProducer, Consumer extends KafkaConsumer>{

    protected Consumer consumer;
    protected Producer producer;

    public void sendAndWait(HealthCheck healthCheck) {

        if (hasHealthCheck(healthCheck)) {

            final MonitorService monitor = new MonitorService();

            int uptime = monitor.getUptime(healthCheck);

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    Configuration.getUptimesTopic(),
                    healthCheck.getSerialNumber(),
                    String.valueOf(uptime));

            Future<RecordMetadata> future = producer.send(record);

            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean hasHealthCheck(HealthCheck healthCheck) {
        return healthCheck != null;
    }


    protected Properties getProducerProperties(String brokers, Class keySerializerClass, Class valueSerializerClass) {
        final Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", brokers);
        producerProperties.put("key.serializer", keySerializerClass);
        producerProperties.put("value.serializer", valueSerializerClass);
        return producerProperties;
    }

    protected Properties getConsumerProperties(String brokers, Class keySerializerClass, Class valueSerializerClass)  {
        final Properties consumerProperties = new Properties();
        consumerProperties.put("group.id", "healthcheck-processor");
        consumerProperties.put("bootstrap.servers", brokers);
        consumerProperties.put("key.deserializer", keySerializerClass);
        consumerProperties.put("value.deserializer", valueSerializerClass);
        return consumerProperties;
    }
}
