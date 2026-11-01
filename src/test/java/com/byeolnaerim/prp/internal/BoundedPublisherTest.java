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
        publisher.complete(); demand.get().request(1);
        assertEquals(1, completions.get());
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
