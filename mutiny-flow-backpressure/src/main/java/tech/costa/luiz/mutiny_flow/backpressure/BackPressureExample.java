package tech.costa.luiz.mutiny_flow.backpressure;

import java.util.concurrent.TimeUnit;

/**
 * The type Back pressure example.
 */
public class BackPressureExample {

    /**
     * Can only consume one item per second r.
     *
     * @param number the number
     * @return the Number
     */
    public static Number canOnlyConsumeOneItemPerSecond(Number number) {
        try {
            // Starting doing a heavy work during 1 second
            TimeUnit.SECONDS.sleep(1);
        } catch (Exception exception) {
            throw new IllegalStateException(exception.getMessage());
        }
        return number;
    }
}
