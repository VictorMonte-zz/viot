package viot.faker;

import com.github.javafaker.Faker;
import viot.domain.HealthCheck;
import viot.domain.MachineStatus;
import viot.domain.MachineType;

import java.util.concurrent.TimeUnit;

public class HealthCheckFaker {

    private static final Faker faker = new Faker();

    public static HealthCheck create() {
        return new HealthCheck(
                "HEALTH_CHECK",
                faker.address().city(),
                faker.bothify("??##-??##", true),
                MachineType.values()[faker.number().numberBetween(0,4)].toString(),
                MachineStatus.values()[faker.number().numberBetween(0,3)].toString(),
                faker.date().past(100, TimeUnit.DAYS),
                faker.number().numberBetween(100L, 0L),
                faker.internet().ipV4Address());
    }
}
