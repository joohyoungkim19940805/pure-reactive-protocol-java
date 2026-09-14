package com.byeolnaerim.prp.internal;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.function.LongConsumer;

public final class BoundedPublisher<T> implements Flow.Publisher<T>, AutoCloseable {
    private final int capacity;
    private final LongConsumer requestHook;
    private final Runnable cancelHook;
    private final ArrayDeque<T> queue = new ArrayDeque<>();
    private Flow.Subscriber<? super T> subscriber;
    private long demand;
    private boolean subscribed;
    private boolean cancelled;
    private boolean completed;
    private Throwable failure;
    private boolean draining;

    public BoundedPublisher(int capacity) {
        this(capacity, ignored -> {}, () -> {});
    }

    public BoundedPublisher(int capacity, LongConsumer requestHook) {
        this(capacity, requestHook, () -> {});
    }

    public BoundedPublisher(int capacity, LongConsumer requestHook, Runnable cancelHook) {
        if (capacity <= 0) throw new IllegalArgumentException("Publisher capacity must be positive.");
        this.capacity = capacity;
        this.requestHook = Objects.requireNonNull(requestHook);
        this.cancelHook = Objects.requireNonNull(cancelHook);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super T> nextSubscriber) {
        Objects.requireNonNull(nextSubscriber);
        synchronized (this) {
            if (subscribed) {
                nextSubscriber.onSubscribe(new Flow.Subscription() {
                    @Override public void request(long n) { nextSubscriber.onError(new IllegalStateException("PRP publishers are unicast.")); }
                    @Override public void cancel() {}
                });
                return;
            }
            subscribed = true;
            subscriber = nextSubscriber;
        }
        nextSubscriber.onSubscribe(new Flow.Subscription() {
            private boolean ended;

            @Override
            public void request(long n) {
                if (ended) return;
                if (n <= 0) {
                    ended = true;
                    fail(new IllegalArgumentException("Reactive Streams demand must be positive."));
                    return;
                }
                synchronized (BoundedPublisher.this) {
                    if (cancelled) return;
                    demand = addDemand(demand, n);
                }
                try { requestHook.accept(n); }
                catch (Throwable error) { fail(error); return; }
                drain();
            }

            @Override
            public void cancel() {
                if (ended) return;
                ended = true;
                synchronized (BoundedPublisher.this) {
                    if (cancelled) return;
                    cancelled = true;
                    queue.clear();
                    BoundedPublisher.this.notifyAll();
                }
                cancelHook.run();
            }
        });
        drain();
    }

    public boolean emit(T item) {
        synchronized (this) {
            if (completed || failure != null || cancelled) return false;
            if (queue.size() >= capacity) return false;
            queue.addLast(item);
        }
        drain();
        return true;
    }

    public boolean emitWait(T item) throws InterruptedException {
        synchronized (this) {
            while (!completed && failure == null && !cancelled && queue.size() >= capacity) wait();
            if (completed || failure != null || cancelled) return false;
            queue.addLast(item);
        }
        drain();
        return true;
    }

    public void complete() {
        synchronized (this) {
            if (completed || failure != null || cancelled) return;
            completed = true;
            notifyAll();
        }
        drain();
    }

    public void fail(Throwable error) {
        Flow.Subscriber<? super T> target;
        synchronized (this) {
            if (failure != null || cancelled) return;
            failure = Objects.requireNonNull(error);
            queue.clear();
            notifyAll();
            target = subscriber;
            if (target == null || draining) return;
            draining = true;
        }
        try { target.onError(error); }
        finally { synchronized (this) { draining = false; } }
    }

    public synchronized int size() { return queue.size(); }
    public synchronized boolean hasSubscriber() { return subscriber != null && !cancelled; }

    @Override public void close() { complete(); }

    private void drain() {
        synchronized (this) {
            if (draining) return;
            draining = true;
        }
        try {
            while (true) {
                Flow.Subscriber<? super T> target;
                T item = null;
                Throwable error = null;
                boolean finish = false;
                synchronized (this) {
                    target = subscriber;
                    if (target == null || cancelled) return;
                    if (failure != null) {
                        error = failure;
                        failure = null;
                        cancelled = true;
                    } else if (demand > 0 && !queue.isEmpty()) {
                        item = queue.removeFirst();
                        notifyAll();
                        if (demand != Long.MAX_VALUE) demand -= 1;
                    } else if (completed && queue.isEmpty()) {
                        finish = true;
                        cancelled = true;
                    } else {
                        return;
                    }
                }
                if (error != null) { target.onError(error); return; }
                if (finish) { target.onComplete(); return; }
                target.onNext(item);
            }
        } finally {
            synchronized (this) { draining = false; }
        }
    }

    private static long addDemand(long current, long additional) {
        long result = current + additional;
        return result < 0 ? Long.MAX_VALUE : result;
    }
}
