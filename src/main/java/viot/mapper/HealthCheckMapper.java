package viot.mapper;

import org.apache.avro.generic.GenericRecord;
import viot.domain.HealthCheck;

import java.util.Date;

public class HealthCheckMapper {
    public static HealthCheck map(GenericRecord healthCheckAvro) {
        return new HealthCheck(
                healthCheckAvro.get("event").toString(),
                healthCheckAvro.get("factory").toString(),
                healthCheckAvro.get("serialNumber").toString(),
                healthCheckAvro.get("type").toString(),
                healthCheckAvro.get("status").toString(),
                new Date((Long) healthCheckAvro.get("lastStartedAt")),
                Float.parseFloat(healthCheckAvro.get("temperature").toString()),
                healthCheckAvro.get("ipAddress").toString());
    }
}
