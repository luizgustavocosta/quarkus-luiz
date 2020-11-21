package tech.costa.luiz.mutiny_flow.backpressure;

import java.util.concurrent.TimeUnit;

public class BackPressureExample {

    public static <R extends Number> R canOnlyConsumeOneItemPerSecond(Long number) {
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (Exception exception) {
            throw new IllegalStateException(exception.getMessage());
        }
        return (R) number;
    }
}
