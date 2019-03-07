package viot.infraestructure;

public class Configuration {
    public static String getHealthCheckTopic() {
        return "healthchecks";
    }

    public static String getUptimesTopic() {
        return "uptimes";
    }

    public static String getHealthChecksAvroTopic() {
        return "healthchecks-avro";
    }
}
