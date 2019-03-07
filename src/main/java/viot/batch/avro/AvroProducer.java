package viot.batch.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.domain.HealthCheck;
import viot.faker.HealthCheckFaker;
import viot.infraestructure.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AvroProducer {

    private final KafkaProducer<String, GenericRecord> producer;
    private Schema schema;

    public AvroProducer(String brokers, String schemaRegistryUrl) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("key.serializer", StringSerializer.class);
        properties.put("value.serializer", KafkaAvroSerializer.class);
        properties.put("schema.registry.url", schemaRegistryUrl);
        producer = new KafkaProducer<>(properties);

        try {
            schema = (new Schema.Parser()).parse(new File("src/main/resources/healthcheck.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public final void produce(int ratePerSecond) {

        long waitTimeBetweenIterationsMs = 1000L / (long)ratePerSecond;

        while(true) {

            HealthCheck fakeHealthCheck = HealthCheckFaker.create();

            GenericData.Record avroHealthCheck = mapToAvro(fakeHealthCheck);

            Future futureResult = producer.send(
                new ProducerRecord<>(Configuration.getHealthChecksAvroTopic(), avroHealthCheck)
            );

            try {
                Thread.sleep(waitTimeBetweenIterationsMs);
                futureResult.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private GenericData.Record mapToAvro(HealthCheck fakeHealthCheck) {
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        recordBuilder.set("event", fakeHealthCheck.getEvent());
        recordBuilder.set("factory", fakeHealthCheck.getFactory());
        recordBuilder.set("serialNumber", fakeHealthCheck.getSerialNumber());
        recordBuilder.set("type", fakeHealthCheck.getType());
        recordBuilder.set("status", fakeHealthCheck.getStatus());
        recordBuilder.set("lastStartedAt", fakeHealthCheck.getLastStartedAt().getTime());
        recordBuilder.set("temperature", fakeHealthCheck.getTemperature());
        recordBuilder.set("ipAddress", fakeHealthCheck.getIpAddress());
        return recordBuilder.build();
    }

    public static void main( String[] args) {
        new AvroProducer("localhost:29092","http://localhost:29081").produce(2);
    }
}
