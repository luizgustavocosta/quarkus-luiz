package tech.costa.luiz.mutiny_flow.backpressure;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.*;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;

/**
 * The original code is available on https://quarkus.io/blog/mutiny-back-pressure/
 */
@DisplayName("Flow control and Back-pressure")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BackPressureTest implements WithAssertions {

    private static final int MILLIS = 10, START_INCLUSIVE = 0, END_EXCLUSIVE = 10;
    private static final int NUMBER_OF_ITEMS = 10;

    private static boolean consume(Object consumeTheStream) { return true; }

    private Multi<Integer> createItemsByRange(int start, int end) {
        return Multi.createFrom().range(start, end);
    }

    private Multi<Long> createItemUsingTheInterval(int millis) {
        return Multi.createFrom().ticks().every(Duration.ofMillis(millis));
    }

    /**
     * Cant consume events.
     */
    @Test
    @Order(1)
    @DisplayName("MissingBackPressureFailure")
    void cantConsumeEvents() {
        final Multi<Object> streaming = createItemUsingTheInterval(MILLIS)
                .emitOn(Infrastructure.getDefaultExecutor())
                .onItem().transform(BackPressureExample::canOnlyConsumeOneItemPerSecond);

        streaming.subscribe().with(
                item -> System.out.println("Got item: " + item),
                failure -> System.out.println("Got failure: " + failure));

        StepVerifier
                .create(streaming)
                .expectNextCount(1L)
                .expectErrorMatches(throwable -> throwable instanceof BackPressureFailure &&
                        throwable.getMessage().equals("Could not emit tick 16 due to lack of requests")
                ).verify();
    }

    /**
     * Buffering items.
     */
    @Test
    @Order(2)
    @DisplayName("Buffering items")
    void bufferingItems() {
        int bufferSize = 250;
        final Multi<Object> usingBuffer = createItemUsingTheInterval(MILLIS)
                .onOverflow().buffer(bufferSize)
                .emitOn(Infrastructure.getDefaultExecutor())
                .onItem().transform(BackPressureExample::canOnlyConsumeOneItemPerSecond);

        usingBuffer.subscribe().with(
                item -> System.out.println("Got item: " + item),
                failure -> System.out.println("Got failure: " + failure));

        long nextCountExpected = 3L;
        StepVerifier
                .create(usingBuffer)
                .expectNextCount(nextCountExpected)
                .expectErrorMatches(throwable -> throwable instanceof BackPressureFailure &&
                        throwable.getMessage().equals("Buffer is full due to lack of downstream consumption")
                ).verify();
    }

    /**
     * Dropping not consumed items.
     */
    @Test
    @Order(3)
    @DisplayName("Dropping items")
    void droppingNotConsumedItems() {
        final Multi<Number> overflowItems = createItemUsingTheInterval(MILLIS)
                .onOverflow().drop(item -> System.out.println("Dropping item " + item))
                .emitOn(Infrastructure.getDefaultExecutor())
                .onItem().transform(BackPressureExample::canOnlyConsumeOneItemPerSecond)
                .transform()
                .byTakingFirstItems(NUMBER_OF_ITEMS);

        StepVerifier
                .create(overflowItems)
                .recordWith(ArrayList::new)
                .thenConsumeWhile(BackPressureTest::consume)
                .consumeRecordedWith(elements -> {
                    assertThat(elements.size()).as("The size should be greater than one").isEqualTo(NUMBER_OF_ITEMS);
                })
                .expectComplete();
    }

    /**
     * Consume all events.
     */
    @Test
    @Order(4)
    @DisplayName("Handle back-pressure, first approach")
    void consumeAllEvents() {
        final Multi<Integer> backPressureApproach = createItemsByRange(START_INCLUSIVE, END_EXCLUSIVE)
                .onSubscribe().invoke(subscription -> System.out.println("Received subscription: " + subscription))
                .onRequest().invoke(consumer -> System.out.println("Got a request: " + consumer))
                .transform().byFilteringItemsWith(i -> i % 2 == 0)
                .onItem().transform(index -> index * 100);

        StepVerifier
                .create(backPressureApproach)
                .thenConsumeWhile(BackPressureTest::consume)
                .expectComplete()
                .verifyThenAssertThat()
                .hasNotDroppedElements();
    }

    /**
     * Handle back pressure second approach.
     */
    @Test
    @Order(5)
    @DisplayName("Handle back-pressure, second approach")
    void handleBackPressureSecondApproach() {
        // https://github.com/ReactiveX/RxJava/issues/5022
        //upstream                     downstream
        //source <------------- operator (parameters) -------------> consumer/further operators
        final Multi<Object> backPressureApproach = createItemsByRange(START_INCLUSIVE, END_EXCLUSIVE)
                .onSubscribe().invoke(subscription -> System.out.println("Received subscription: " + subscription))
                .onRequest().invoke(request -> System.out.println("Got a request: " + request))
                .onItem().transform(number -> number * 100);

        backPressureApproach.subscribe().with(
                subscription -> {
                    // Got the subscription
                    //upstream.set(subscription);
                    subscription.request(1L);
                },
                item -> {
                    System.out.println("item: " + item);

                    //upstream.get().request(1);
                },
                throwable -> System.out.println("Failed with " + throwable),
                () -> System.out.println("Completed")
        );

        StepVerifier
                .create(backPressureApproach)
                .expectNext(0, 100, 200, 300, 400, 500,600, 700, 800, 900)
                .expectComplete()
                .verifyThenAssertThat()
                .hasNotDiscardedElements();
                //.hasNotDroppedElements();
    }

    /**
     * When receive a stream create your own subscriber.
     */
    @Test
    @Order(6)
    @DisplayName("Chuck Norris mode a.k.a. Coding your own subscriber")
    void whenReceiveAStreamCreateYourOwnSubscriber() {
        final Multi<Number> chuckNorris = createItemsByRange(START_INCLUSIVE, END_EXCLUSIVE)
                .onSubscribe().invoke(sub -> System.out.println("Received subscription: " + sub))
                .onRequest().invoke(req -> System.out.println("Got a request: " + req))
                .onItem().transform(BackPressureExample::canOnlyConsumeOneItemPerSecond);

        final long receivedItems = chuckNorris.subscribe()
                .withSubscriber(new OwnNumberSubscriber<>(1L))
                .count();

        assertThat(END_EXCLUSIVE)
                .as("Send items should be equals to received items")
                .isEqualTo(receivedItems);
    }
}