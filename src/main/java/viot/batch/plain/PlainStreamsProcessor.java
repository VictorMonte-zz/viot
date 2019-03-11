package viot.batch.plain;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import viot.domain.HealthCheck;
import viot.infraestructure.Configuration;
import viot.service.MonitorService;
import viot.wrapper.ObjectMapperWrapper;

import java.util.Properties;

public class PlainStreamsProcessor {

    private static final Logger logger = LogManager.getLogger();
    private final String brokers;

    public PlainStreamsProcessor(String brokers) {
        this.brokers = brokers;
    }

    public final void process() {

        Topology topology = createTopology();
        Properties properties = getProperties();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.start();

        logger.info("Kafka stream started");

    }

    private Topology createTopology() {

        logger.info("Creating kafka stream topology");

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
                .stream(Configuration.getHealthCheckTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((v -> ObjectMapperWrapper.read(v, HealthCheck.class)))
                .map((k, v) -> toUptimeStream(v))
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

        (new PlainStreamsProcessor("localhost:29092")).process();

    }
}
