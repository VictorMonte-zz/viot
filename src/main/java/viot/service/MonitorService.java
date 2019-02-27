package viot.service;

import viot.domain.HealthCheck;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;

public class MonitorService {

    public int getUptime(HealthCheck healthCheck) {

        LocalDate startDateLocal = healthCheck
                .getLastStartedAt()
                .toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();

        int uptime = Period.between(startDateLocal, LocalDate.now()).getDays();

        return uptime;
    }
}
