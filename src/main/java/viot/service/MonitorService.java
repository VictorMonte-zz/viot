package viot.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import viot.domain.HealthCheck;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;

public class MonitorService {

    private static final Logger logger = LogManager.getLogger();

    public int getUptime(HealthCheck healthCheck) {

        logger.info("Calculating time for {}", healthCheck);

        LocalDate startDateLocal = healthCheck
                .getLastStartedAt()
                .toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDate();

        int uptime = Period.between(startDateLocal, LocalDate.now()).getDays();

        return uptime;
    }
}
