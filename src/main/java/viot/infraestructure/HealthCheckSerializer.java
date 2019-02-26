package viot.infraestructure;

import org.apache.kafka.common.serialization.Serializer;
import viot.helper.ObjectMapperWrapper;

import java.util.Map;

public class HealthCheckSerializer implements Serializer {

    @Override
    public void configure(Map configs, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, Object data) {

        if (data == null) {
            return null;
        }

        return ObjectMapperWrapper.write(data);
    }

    @Override
    public void close() { }

}
