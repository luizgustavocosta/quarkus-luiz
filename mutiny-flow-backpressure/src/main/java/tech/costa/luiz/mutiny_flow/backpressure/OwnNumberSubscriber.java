package tech.costa.luiz.mutiny_flow.backpressure;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicLong;

public class OwnNumberSubscriber<T extends Number> implements Subscriber<T> {

    private Subscription subscription;

    private final AtomicLong receivedItems = new AtomicLong();
    private final long itemsToRequest;

    public OwnNumberSubscriber(long itemsToRequest) {
        this.itemsToRequest = itemsToRequest;
        System.out.println("I'm tough..doing by my own");
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(itemsToRequest);
    }

    @Override
    public void onNext(T item) {
        System.out.println("Got item " + item);
        receivedItems.incrementAndGet();
        subscription.request(itemsToRequest);
    }

    @Override
    public void onError(Throwable throwable) {
        System.err.println(throwable);
    }

    @Override
    public void onComplete() {
        System.out.println("Complete");
    }

    public long count() {
        return receivedItems.get();
    }
}
