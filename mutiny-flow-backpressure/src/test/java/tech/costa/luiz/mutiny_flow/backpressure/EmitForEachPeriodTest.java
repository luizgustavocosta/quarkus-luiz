package tech.costa.luiz.mutiny_flow.backpressure;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;

class EmitForEachPeriodTest {

    @Test
    void cantConsumeAllEvents() {
        final Multi<Object> multi = Multi.createFrom().ticks().every(Duration.ofMillis(10))
                .emitOn(Infrastructure.getDefaultExecutor())
                .onItem().transform(BackPressureExample::canOnlyConsumeOneItemPerSecond);

        StepVerifier
                .create(multi)
                .expectNextCount(1)
                .expectErrorMatches(throwable -> throwable instanceof BackPressureFailure &&
                        throwable.getMessage().equals("Could not emit tick 16 due to lack of requests")
                ).verify();

    }
}