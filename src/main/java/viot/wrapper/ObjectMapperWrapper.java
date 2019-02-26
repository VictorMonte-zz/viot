package viot.wrapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;

import java.io.IOException;

public class ObjectMapperWrapper {

    private static ObjectMapper jsonMapper;

    static {
        jsonMapper =
                new ObjectMapper()
                        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                        .setDateFormat(new StdDateFormat());
    }

    public static <T> String convert(T object) {
        String result = null;
        try {
            result = jsonMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static <T> T read(String json, Class<T> type) {
        try {
            return jsonMapper.readValue(json, type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] write(Object data) {
        try {
            return jsonMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public static <T> T read(byte[] source, Class<T> type) {
        try {
            jsonMapper.readValue(source, type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
