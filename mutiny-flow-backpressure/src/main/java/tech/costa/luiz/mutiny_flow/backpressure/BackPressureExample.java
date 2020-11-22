package tech.costa.luiz.mutiny_flow.backpressure;

import java.util.concurrent.TimeUnit;

/**
 * The type Back pressure example.
 */
public class BackPressureExample {

    /**
     * Can only consume one item per second r.
     *
     * @param <R>    the type parameter
     * @param number the number
     * @return the r
     */
    public static <R extends Number> R canOnlyConsumeOneItemPerSecond(Long number) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception exception) {
            throw new IllegalStateException(exception.getMessage());
        }
        return (R) number;
    }
}
