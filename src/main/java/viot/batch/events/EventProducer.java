package viot.batch.events;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import viot.infraestructure.Configuration;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class EventProducer {

    public static final int LATE_MESSAGE_ID = 54;
    private final KafkaProducer<String, String> producer;

    public EventProducer(String brokers) {

        Properties props = getProperties(brokers);
        producer = new KafkaProducer<>(props);
    }

    private Properties getProperties(String brokers) {

        Properties props = new Properties();

        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        return props;
    }

    private void sendMessage(long id, long currentTime, String info) {

        final long window = currentTime / 10000 * 10000;
        final String value = "" + window + "," +  id + "," + info;

        final ProducerRecord<String, String> record =
                new ProducerRecord<>(Configuration.getEventsTopic(), null, currentTime, String.valueOf(id), value);

        Future<RecordMetadata> future = this.producer.send(record);

        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    public void produce(){

        long now = System.currentTimeMillis();
        long delay = 1300 - Math.floorMod(now, 1000);
        Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            @Override
            public void run() {

                long currentTime = System.currentTimeMillis();
                long second = Math.floorMod(currentTime / 1000, 60);

                // should not send 54th
                if (second != LATE_MESSAGE_ID) {
                    EventProducer.this.sendMessage(second, currentTime, "on time");
                }

                // should send missing 54th with 6th
                if (second == 6) {
                    EventProducer.this.sendMessage(LATE_MESSAGE_ID, currentTime, "late");
                }

            }
        }, delay, 1000);
    }

    public static void main(String[] args) {
        (new EventProducer("localhost:29092")).produce();
    }
}
