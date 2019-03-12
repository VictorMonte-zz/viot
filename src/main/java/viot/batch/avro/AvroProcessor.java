package viot.batch.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.batch.BaseProcessor;
import viot.domain.HealthCheck;
import viot.infraestructure.Configuration;
import viot.mapper.HealthCheckMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class AvroProcessor extends BaseProcessor<KafkaProducer<String, String>, KafkaConsumer<String, GenericRecord>>{

    public AvroProcessor(String brokers , String schemaRegistryUrl) {

        final Properties consumerProps = getConsumerProperties(brokers, StringDeserializer.class, KafkaAvroDeserializer.class);
        addAvroConsumerProperties(schemaRegistryUrl, consumerProps);
        consumer = new KafkaConsumer<>(consumerProps);

        final Properties producerProps = getProducerProperties(brokers, StringSerializer.class, StringSerializer.class);
        producer = new KafkaProducer<>(producerProps);
    }

    private void addAvroConsumerProperties(String schemaRegistryUrl, Properties consumerProps) {
        consumerProps.put("schema.registry.url", schemaRegistryUrl);
    }

    public final void process() {

        consumer.subscribe(Collections.singletonList(Configuration.getHealthChecksAvroTopic()));

        while(true) {

            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1L));

            for(ConsumerRecord<String, GenericRecord> record : records) {
                HealthCheck healthCheck = mapToHealthCheck(record);
                sendAndWait(healthCheck);
            }
        }
    }

    private HealthCheck mapToHealthCheck(ConsumerRecord<String, GenericRecord> record) {

        GenericRecord healthCheckAvro = record.value();

        final HealthCheck healthCheck = HealthCheckMapper.map(healthCheckAvro);

        return healthCheck;
    }

    public static void main(String[] args) {
        new AvroProcessor("localhost:29092","http://localhost:29081").process();//7
    }
}
