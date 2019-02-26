import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.domain.HealthCheck;
import viot.helper.ObjectMapperWrapper;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PlainProcessor {

    private final KafkaConsumer<String, String> consumer;
    private final KafkaProducer<String, String> producer;

    public PlainProcessor(String brokers) {

        Properties consumerProps = new Properties();
        consumerProps.put("group.id", "healthcheck-processor");
        consumerProps.put("bootstrap.servers", brokers);
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", StringDeserializer.class);
        consumer = new KafkaConsumer<>(consumerProps);

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", brokers);
        producerProps.put("key.serializer", StringSerializer.class);
        producerProps.put("value.serializer", StringSerializer.class);
        producer = new KafkaProducer<>(producerProps);

    }

    public void process() {

        consumer.subscribe(Collections.singletonList("healthchecks"));

        while(true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));

            for (ConsumerRecord<String, String> record : records) {

                String json = record.value();
                HealthCheck healthCheck = ObjectMapperWrapper.read(json, HealthCheck.class);

                LocalDate startDateLocal = healthCheck
                        .getLastStartedAt()
                        .toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate();

                int days = Period.between(startDateLocal, LocalDate.now()).getDays();

                Future<RecordMetadata> future = producer.send(
                        new ProducerRecord<>("uptimes", healthCheck.getSerialNumber(), String.valueOf(days))
                );

                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main( String[] args) {
        (new PlainProcessor("localhost:9092")).process();
    }
}
