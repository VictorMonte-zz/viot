package viot.domain;

import java.util.Date;

public class HealthCheck {
    private String event;
    private String factory;
    private String serialNumber;
    private String type;
    private String status;
    private Date lastStartedAt;
    private float temperature;
    private String ipAddress;

    public HealthCheck(String event, String factory, String serialNumber, String type, String status, Date lastStartedAt, float temperature, String ipAddress) {
        this.event = event;
        this.factory = factory;
        this.serialNumber = serialNumber;
        this.type = type;
        this.status = status;
        this.lastStartedAt = lastStartedAt;
        this.temperature = temperature;
        this.ipAddress = ipAddress;
    }
}
