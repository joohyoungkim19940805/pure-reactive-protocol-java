package com.byeolnaerim.prp.internal;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class BoundedPublisherTest {
    @Test
    void concurrentProducersDoNotLeaveAcceptedItemsWithoutADrainer() throws Exception {
        int producerCount = 4, itemsPerProducer = 100_000, total = producerCount * itemsPerProducer;
        BoundedPublisher<Integer> publisher = new BoundedPublisher<>(1);
        AtomicInteger received = new AtomicInteger(), activeCallbacks = new AtomicInteger(), maximumCallbacks = new AtomicInteger();
        AtomicInteger terminalCount = new AtomicInteger();
        AtomicIntegerArray seen = new AtomicIntegerArray(total);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        CompletableFuture<Void> terminal = new CompletableFuture<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(Long.MAX_VALUE); }
            private void enter() { maximumCallbacks.accumulateAndGet(activeCallbacks.incrementAndGet(), Math::max); }
            @Override public void onNext(Integer item) {
                enter();
                try {
                    if (item == null || item < 0 || item >= total || seen.getAndIncrement(item) != 0)
                        callbackFailure.compareAndSet(null, new AssertionError("Duplicated or invalid item: " + item));
                    received.incrementAndGet();
                } finally { activeCallbacks.decrementAndGet(); }
            }
            @Override public void onError(Throwable error) {
                enter();
                try { terminalCount.incrementAndGet(); terminal.completeExceptionally(error); }
                finally { activeCallbacks.decrementAndGet(); }
            }
            @Override public void onComplete() {
                enter();
                try { terminalCount.incrementAndGet(); terminal.complete(null); }
                finally { activeCallbacks.decrementAndGet(); }
            }
        });
        CountDownLatch started = new CountDownLatch(producerCount), start = new CountDownLatch(1);
        ExecutorService producers = Executors.newFixedThreadPool(producerCount, task -> {
            Thread thread = new Thread(task, "bounded-publisher-producer"); thread.setDaemon(true); return thread;
        });
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int producer = 0; producer < producerCount; producer++) {
                int base = producer * itemsPerProducer;
                tasks.add(producers.submit(() -> {
                    started.countDown();
                    try {
                        start.await();
                        for (int item = 0; item < itemsPerProducer; item++)
                            if (!publisher.emitWait(base + item)) throw new AssertionError("Publisher rejected an item before completion");
                    } catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new RuntimeException(error); }
                }));
            }
            assertTrue(started.await(2, TimeUnit.SECONDS));
            start.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            for (Future<?> task : tasks) {
                long remaining = Math.max(1, deadline - System.nanoTime());
                assertDoesNotThrow(() -> task.get(remaining, TimeUnit.NANOSECONDS),
                    () -> "Accepted items stopped draining despite unbounded demand: received=" + received.get() + ", queued=" + publisher.size());
            }
            assertNull(callbackFailure.get());
            assertEquals(total, received.get());
            assertEquals(0, publisher.size());
            publisher.complete();
            terminal.get(2, TimeUnit.SECONDS);
            publisher.complete();
            assertEquals(1, terminalCount.get());
            assertEquals(1, maximumCallbacks.get(), "Subscriber callbacks must remain serialized");
            assertFalse(publisher.emit(total));
        } finally {
            // Also release an unfixed publisher's stalled emitWait callers when
            // this regression fails; leave no producer threads behind.
            start.countDown(); publisher.complete();
            for (Future<?> task : tasks) task.cancel(true);
            producers.shutdownNow();
            assertTrue(producers.awaitTermination(2, TimeUnit.SECONDS), "Producer cleanup did not finish");
        }
    }

    @Test
    void bufferedCompletionWaitsForDemandAndSignalsTerminalOnce() {
        BoundedPublisher<Integer> publisher = new BoundedPublisher<>(1);
        List<Integer> received = new ArrayList<>();
        AtomicReference<Flow.Subscription> demand = new AtomicReference<>();
        AtomicInteger completions = new AtomicInteger();
        assertTrue(publisher.emit(1));
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { demand.set(subscription); }
            @Override public void onNext(Integer item) { received.add(item); }
            @Override public void onError(Throwable error) { fail(error); }
            @Override public void onComplete() { completions.incrementAndGet(); }
        });
        publisher.complete();
        assertTrue(received.isEmpty());
        assertEquals(0, completions.get());
        demand.get().request(1);
        assertEquals(List.of(1), received);
        assertEquals(1, completions.get());
        publisher.fail(new IllegalStateException("Failure after completion"));
        publisher.complete(); demand.get().request(1);
        assertEquals(1, completions.get());
    }

    @Test
    void idleFailureSignalsOnceAndRejectsSubsequentWork() throws Exception {
        BoundedPublisher<Integer> publisher = new BoundedPublisher<>(1);
        AtomicReference<Flow.Subscription> demand = new AtomicReference<>();
        AtomicInteger errors = new AtomicInteger(), completions = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("Publisher failed");
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { demand.set(subscription); }
            @Override public void onNext(Integer item) { fail("Failure must discard buffered items"); }
            @Override public void onError(Throwable error) { assertSame(failure, error); errors.incrementAndGet(); }
            @Override public void onComplete() { completions.incrementAndGet(); }
        });
        assertTrue(publisher.emit(1));
        publisher.fail(failure);
        assertEquals(1, errors.get());
        demand.get().request(1);
        publisher.complete(); publisher.fail(new IllegalStateException("Late failure"));
        demand.get().request(1);
        assertEquals(1, errors.get(), "Requests after failure must not repeat onError");
        assertEquals(0, completions.get());
        assertFalse(publisher.emit(2));
        assertFalse(publisher.emitWait(2));
        assertEquals(0, publisher.size());
        assertFalse(publisher.hasSubscriber());
    }

    @Test
    void failureBeforeSubscriptionIsDeliveredOnceWithoutDemand() {
        BoundedPublisher<Integer> publisher = new BoundedPublisher<>(1);
        AtomicReference<Flow.Subscription> demand = new AtomicReference<>();
        AtomicInteger errors = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("Failed before subscription");
        assertTrue(publisher.emit(1));
        publisher.fail(failure);
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { demand.set(subscription); }
            @Override public void onNext(Integer item) { fail("Failure must discard buffered items"); }
            @Override public void onError(Throwable error) { assertSame(failure, error); errors.incrementAndGet(); }
            @Override public void onComplete() { fail("Expected failure"); }
        });
        assertEquals(1, errors.get());
        demand.get().request(1); publisher.fail(failure);
        assertEquals(1, errors.get());
        assertEquals(0, publisher.size());
    }

    @Test
    void reentrantFailureWaitsForOnNextBeforeSignallingOneError() {
        BoundedPublisher<Integer> publisher = new BoundedPublisher<>(1);
        AtomicReference<Flow.Subscription> demand = new AtomicReference<>();
        AtomicInteger items = new AtomicInteger(), errors = new AtomicInteger(), activeCallbacks = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("Reentrant failure");
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { demand.set(subscription); subscription.request(Long.MAX_VALUE); }
            @Override public void onNext(Integer item) {
                assertEquals(0, activeCallbacks.getAndIncrement());
                try { items.incrementAndGet(); publisher.fail(failure); }
                finally { activeCallbacks.decrementAndGet(); }
            }
            @Override public void onError(Throwable error) {
                assertEquals(0, activeCallbacks.get());
                assertSame(failure, error); errors.incrementAndGet();
            }
            @Override public void onComplete() { fail("Expected failure"); }
        });
        assertTrue(publisher.emit(1));
        demand.get().request(1);
        assertEquals(1, items.get());
        assertEquals(1, errors.get());
        assertFalse(publisher.emit(2));
    }

    @Test
    void failureReleasesAProducerWaitingForQueueCapacity() throws Exception {
        BoundedPublisher<Integer> publisher = new BoundedPublisher<>(1);
        assertTrue(publisher.emit(1));
        CountDownLatch started = new CountDownLatch(1);
        CompletableFuture<Boolean> outcome = new CompletableFuture<>();
        Thread producer = new Thread(() -> {
            started.countDown();
            try { outcome.complete(publisher.emitWait(2)); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); outcome.completeExceptionally(error); }
        }, "bounded-publisher-failure-waiter");
        producer.setDaemon(true);
        producer.start();
        try {
            assertTrue(started.await(2, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (producer.getState() != Thread.State.WAITING && System.nanoTime() < deadline)
                java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
            assertEquals(Thread.State.WAITING, producer.getState(), "Producer must be waiting on the full queue");
            publisher.fail(new IllegalStateException("Release waiting producer"));
            assertFalse(outcome.get(2, TimeUnit.SECONDS));
            assertEquals(0, publisher.size());
        } finally {
            producer.interrupt(); producer.join(2000);
            assertFalse(producer.isAlive(), "Producer cleanup did not finish");
        }
    }

    @Test
    void reentrantEmissionAndCompletionKeepOneDrainOwner() {
        BoundedPublisher<Integer> publisher = new BoundedPublisher<>(1);
        AtomicInteger received = new AtomicInteger(), callbacks = new AtomicInteger(), maximum = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(Long.MAX_VALUE); }
            @Override public void onNext(Integer item) {
                maximum.accumulateAndGet(callbacks.incrementAndGet(), Math::max);
                try {
                    assertEquals(received.getAndIncrement(), item);
                    if (item < 999) assertTrue(publisher.emit(item + 1)); else publisher.complete();
                } finally { callbacks.decrementAndGet(); }
            }
            @Override public void onError(Throwable error) { fail(error); }
            @Override public void onComplete() { completions.incrementAndGet(); }
        });
        assertTrue(publisher.emit(0));
        assertEquals(1000, received.get());
        assertEquals(1, maximum.get());
        assertEquals(1, completions.get());
        assertEquals(0, publisher.size());
    }
}
