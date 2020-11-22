package tech.costa.luiz.mutiny_flow.backpressure;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.BackPressureFailure;
import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.ArrayList;

/**
 * The original code is available on https://quarkus.io/blog/mutiny-back-pressure/
 */
@DisplayName("Flow control and Back-pressure")
class BackPressureTest implements WithAssertions {

    private static final int MILLIS = 10, START_INCLUSIVE = 0, END_EXCLUSIVE = 10;
    private static final int NUMBER_OF_ITEMS = 10;

    private Multi<Integer> createItemsUsing(int start, int end) {
        return Multi.createFrom().range(start, end);
    }

    private Multi<Long> createItemUsingTheInterval(int millis) {
        return Multi.createFrom().ticks().every(Duration.ofMillis(millis));
    }

    @Test
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

    @Test
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

    @Test
    @DisplayName("Dropping items")
    void droppingNotConsumedItems() {
        final Multi<Object> overflowItems = createItemUsingTheInterval(MILLIS)
                .onOverflow().drop(item -> System.out.println("Dropping item " + item))
                .emitOn(Infrastructure.getDefaultExecutor())
                .onItem().transform(BackPressureExample::canOnlyConsumeOneItemPerSecond)
                .transform()
                .byTakingFirstItems(NUMBER_OF_ITEMS);

        StepVerifier
                .create(overflowItems)
                .recordWith(ArrayList::new)
                .thenConsumeWhile(consumeTheStream -> true)
                .consumeRecordedWith(elements -> {
                    assertThat(elements.size()).as("The size should be greater than one").isEqualTo(NUMBER_OF_ITEMS);
                })
                .expectComplete()
                .verifyThenAssertThat()
                .tookMoreThan(Duration.ofSeconds(50));

    }


    @Test
    @DisplayName("Handle back-pressure, first approach")
    void consumeAllEvents() {
        final Multi<Integer> backPressureApproach = createItemsUsing(START_INCLUSIVE, END_EXCLUSIVE)
                .onSubscribe().invoke(subscription -> System.out.println("Received subscription: " + subscription))
                .onRequest().invoke(consumer -> System.out.println("Got a request: " + consumer))
                .transform().byFilteringItemsWith(i -> i % 2 == 0)
                .onItem().transform(index -> index * 100);

        StepVerifier
                .create(backPressureApproach)
                .thenConsumeWhile(consumeTheStream -> true)
                .expectComplete()
                .verifyThenAssertThat()
                .hasNotDroppedElements();
    }


    @Test
    @DisplayName("Handle back-pressure, second approach")
    void handleBackPressureSecondApproach() {
        // https://github.com/ReactiveX/RxJava/issues/5022
        //upstream                     downstream
        //source <------------- operator (parameters) -------------> consumer/further operators
        final Multi<Object> backPressureApproach = createItemsUsing(START_INCLUSIVE, END_EXCLUSIVE)
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
                .thenConsumeWhile(consumeTheStream -> true)
                .expectComplete()
                .verifyThenAssertThat()
                .tookMoreThan(Duration.ofMillis(5))
                .hasNotDroppedElements();
    }

    @Test
    @DisplayName("Chuck Norris mode")
    void whenReceiveAStreamCreateYourOwnSubscriber() {
        final Multi<Integer> chuckNorris = createItemsUsing(START_INCLUSIVE, END_EXCLUSIVE)
                .onSubscribe().invoke(sub -> System.out.println("Received subscription: " + sub))
                .onRequest().invoke(req -> System.out.println("Got a request: " + req))
                .onItem().transform(number -> number * 100);

        chuckNorris.subscribe()
                .withSubscriber(
                        new Subscriber<Integer>() {
                                    private Subscription subscription;

                                    @Override
                                    public void onSubscribe(Subscription s) {
                                        this.subscription = s;
                                        s.request(1);
                                    }

                                    @Override
                                    public void onNext(Integer item) {
                                        System.out.println("Got item " + item);
                                        subscription.request(1);
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        // ...
                                    }

                                    @Override
                                    public void onComplete() {
                                        System.out.println("Complete");
                                    }
                                }
                );

        StepVerifier
                .create(chuckNorris)
                .expectSubscription()
                .thenConsumeWhile(consumeTheStream -> true)
                .expectComplete()
                .verifyThenAssertThat()
                .tookMoreThan(Duration.ofMillis(5))
                .hasNotDroppedElements();
    }

}