package viot.batch.plain;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.batch.BaseProcessor;
import viot.domain.HealthCheck;
import viot.wrapper.ObjectMapperWrapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PlainProcessor extends BaseProcessor<KafkaProducer<String, String>, KafkaConsumer<String, String>> {

    public PlainProcessor(String brokers) {

        final Properties consumerProps = getConsumerProperties(brokers, StringDeserializer.class, StringDeserializer.class);
        consumer = new KafkaConsumer<>(consumerProps);

        final Properties producerProps = getProducerProperties(brokers, StringSerializer.class, StringSerializer.class);
        producer = new KafkaProducer<>(producerProps);

    }

    public void process() {

        consumer.subscribe(Collections.singletonList("healthchecks"));

        while(true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));

            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
                HealthCheck healthCheck = ObjectMapperWrapper.read(json, HealthCheck.class);
                sendAndWait(healthCheck);
            }
        }
    }

    public static void main( String[] args) {
        (new PlainProcessor("localhost:29092")).process();
    }
}
