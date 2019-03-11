package viot.batch.custom;

import org.apache.kafka.common.serialization.Serde;
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
import viot.infraestructure.HealthCheckDeserializer;
import viot.infraestructure.HealthCheckSerializer;
import viot.service.MonitorService;

import java.util.Properties;

public class CustomStreamsProcessor {

    private final String brokers;

    public CustomStreamsProcessor(String brokers) {
        this.brokers = brokers;
    }

    public final void process() {

        Topology topology = buildTopology();
        Properties properties = getProperties();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.start();

    }

    private Topology buildTopology() {

        final Serde customSerde = Serdes.serdeFrom(new HealthCheckSerializer(), new HealthCheckDeserializer());
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
                .stream(Configuration.getHealthCheckTopic(), Consumed.with(Serdes.String(), customSerde))
                .map((KeyValueMapper) (k, v) -> toUptimeStream((HealthCheck)v))
                .to(Configuration.getUptimesTopic(), Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

    private Properties getProperties() {

        Properties props = new Properties();

        props.put("bootstrap.servers", this.brokers);
        props.put("application.id", "viot");

        return props;
    }

    private KeyValue<String, String> toUptimeStream(HealthCheck v) {

        final MonitorService monitor = new MonitorService();

        int uptime = monitor.getUptime(v);

        return new KeyValue<>(v.getSerialNumber(), String.valueOf(uptime));
    }

    public static void main(String[] args) {
        (new CustomStreamsProcessor("localhost:29092")).process();
    }
}
