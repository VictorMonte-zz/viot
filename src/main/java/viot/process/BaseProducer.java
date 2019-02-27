package viot.process;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import viot.infraestructure.Configuration;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class BaseProducer<K, V> {

    protected KafkaProducer<K, V> producer;
    private static final Logger logger = LogManager.getLogger(BaseProducer.class.getName());

    public void produce(int ratesPerSecond) {

        while (true){

            logger.info("Sending health check message");

            ProducerRecord<K, V> record = new ProducerRecord<>(Configuration.getHealthCheckTopic(), createValue());
            Future<RecordMetadata> future = producer.send(record);

            try {

                long millis = 1000L / ratesPerSecond;
                Thread.sleep(millis);
                future.get();

                logger.info("Message sent with success");

            } catch (InterruptedException | ExecutionException e) {
                logger.error(e);
                e.printStackTrace();
            }

        }
    }

    public abstract V createValue();
}
