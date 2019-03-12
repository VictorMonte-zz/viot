package viot.batch.avro;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import viot.domain.HealthCheck;
import viot.infraestructure.Configuration;
import viot.mapper.HealthCheckMapper;
import viot.service.MonitorService;

import java.util.Collections;
import java.util.Properties;

public class AvroStreamsProcessor {

    private final String brokers;
    private final String schemaRegistryUrl;

    public AvroStreamsProcessor(String brokers, String schemaRegistry) {

        this.brokers = brokers;
        this.schemaRegistryUrl = schemaRegistry;
    }

    public void process() {

        final Topology topology = buildTopology();
        final Properties properties = this.getProperties();

        final KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.start();
    }

    private Properties getProperties() {

        final Properties properties = new Properties();

        properties.put("bootstrap.servers", this.brokers);
        properties.put("application.id", "viot");

        return properties;
    }

    private Topology buildTopology() {

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final GenericAvroSerde avroSerde = getGenericAvroSerde();

        streamsBuilder
                .stream(Configuration.getHealthChecksAvroTopic(), Consumed.with(Serdes.String(), avroSerde))
                .mapValues(v -> HealthCheckMapper.map(v))
                .map((KeyValueMapper)(k, v) -> getUptimeStream((HealthCheck)v))
                .to(Configuration.getUptimesTopic(), Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

    private GenericAvroSerde getGenericAvroSerde() {

        final GenericAvroSerde serde = new GenericAvroSerde();

        serde.configure(Collections.singletonMap("schema.registry.url", schemaRegistryUrl), false);

        return serde;
    }

    private KeyValue<? extends String, ? extends String> getUptimeStream(HealthCheck v) {
        final MonitorService monitorService = new MonitorService();
        final int uptime = monitorService.getUptime(v);
        return new KeyValue<>(v.getSerialNumber(), String.valueOf(uptime));
    }

    public static void main(String[] args) {
        (new AvroStreamsProcessor("localhost:29092", "http://localhost:29081")).process();
    }
}
