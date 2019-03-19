package viot.batch.events;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.*;
import viot.infraestructure.Configuration;

import java.util.Properties;

public class EventProcessor {

    private String brokers;

    public EventProcessor(String brokers) {

        this.brokers = brokers;
    }

    public void process() {

        try {

            final Topology topology = buildTopology();
            final Properties properties = this.getProperties();

            final KafkaStreams streams = new KafkaStreams(topology, properties);

            streams.start();

        } catch (IllegalStateException | StreamsException e) {
            e.printStackTrace();
        }
    }

    private Properties getProperties() {

        final Properties properties = new Properties();

        properties.put("bootstrap.servers", this.brokers);
        properties.put("application.id", "viot");
        properties.put("auto.offset.reset", "latest");
        properties.put("commit.interval.ms", 30000);

        return properties;
    }

    private Topology buildTopology() {

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
                .stream(Configuration.getEventsTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((k, v) -> "foo", Serialized.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(10000L))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .map((ws, i) -> new KeyValue("" + (ws.window().start()), "" + i))
                .to(Configuration.getAggregatesTopic(), Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

    public static void main(String[] args) {
        (new EventProcessor("localhost:29092")).process();
    }
}
