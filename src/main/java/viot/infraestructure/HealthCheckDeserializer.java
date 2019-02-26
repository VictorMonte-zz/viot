package viot.infraestructure;

import org.apache.kafka.common.serialization.Deserializer;
import viot.domain.HealthCheck;
import viot.wrapper.ObjectMapperWrapper;

import java.util.Map;

public class HealthCheckDeserializer implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) { }

    @Override
    public Object deserialize(String topic, byte[] data) {

        if (data == null) {
            return null;
        }

        return ObjectMapperWrapper.read(data, HealthCheck.class);
    }

    @Override
    public void close() { }
}
