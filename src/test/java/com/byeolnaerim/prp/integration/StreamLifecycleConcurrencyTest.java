package com.byeolnaerim.prp.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.ReactiveStream;
import com.byeolnaerim.prp.SessionState;
import com.byeolnaerim.prp.StreamMessage;
import com.byeolnaerim.prp.core.ProtocolLimits;
import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.transport.LaneRequirements;
import com.byeolnaerim.prp.transport.MemoryTransport;
import com.byeolnaerim.prp.transport.ReactiveTransport;
import com.byeolnaerim.prp.transport.TransportCloseEvent;
import com.byeolnaerim.prp.transport.TransportConnection;
import com.byeolnaerim.prp.transport.TransportDescription;
import com.byeolnaerim.prp.transport.TransportLane;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

class StreamLifecycleConcurrencyTest {
    @Test
    void synchronousCompleteDoesNotHoldStreamMonitorWhileWaitingForSession() throws Exception {
        raceIncomingCompleteWithLocalWrite(false);
    }

    @Test
    void synchronousSendDoesNotHoldStreamMonitorWhileWaitingForSession() throws Exception {
        raceIncomingCompleteWithLocalWrite(true);
    }

    private void raceIncomingCompleteWithLocalWrite(boolean send) throws Exception {
        Fixture fixture = new Fixture(false);
        Streams streams = fixture.open();
        Messages localMessages = new Messages(), remoteMessages = new Messages();
        streams.local.subscribe(localMessages);
        streams.remote.subscribe(remoteMessages);
        byte[] payload = new byte[] {1, 2, 3};
        CountDownLatch sessionHeld = new CountDownLatch(1);
        CompletableFuture<Void> writerDone = new CompletableFuture<>(), receiverDone = new CompletableFuture<>();
        Thread writer = daemon("prp-local-" + (send ? "send" : "complete"), writerDone, () -> {
            CompletionStage<Void> result = send ? streams.local.send(payload) : streams.local.complete();
            result.toCompletableFuture().get(2, TimeUnit.SECONDS);
        });
        Thread receiver = daemon("prp-incoming-complete", receiverDone, () -> {
            // Session lifecycle/stream retirement owns this same monitor before
            // consulting a stream. Hold it here to place that ordering exactly,
            // then deliver a real peer COMPLETE through MemoryTransport.
            synchronized (fixture.client) {
                sessionHeld.countDown();
                awaitBlockedOn(writer, fixture.client, Thread.currentThread());
                streams.remote.complete().toCompletableFuture().get(2, TimeUnit.SECONDS);
            }
        });
        receiver.start();
        assertTrue(sessionHeld.await(2, TimeUnit.SECONDS), "Receiver did not acquire the session monitor");
        writer.start();
        // On the unfixed implementation these daemon threads form a real ABBA
        // cycle. Bound the assertion without blocking the JUnit/test JVM thread.
        assertDoesNotThrow(() -> CompletableFuture.allOf(writerDone, receiverDone).get(3, TimeUnit.SECONDS),
            "A stream writer must release its monitor before waiting for the session monitor");
        if (send) {
            assertArrayEquals(payload, remoteMessages.items.poll(2, TimeUnit.SECONDS));
            streams.local.complete().toCompletableFuture().get(2, TimeUnit.SECONDS);
        }
        localMessages.ended.get(2, TimeUnit.SECONDS);
        remoteMessages.ended.get(2, TimeUnit.SECONDS);
        assertTrue(streams.local.closed());
        assertTrue(streams.remote.closed());
        assertEquals(SessionState.READY, fixture.client.state());
        assertEquals(SessionState.READY, fixture.server.state());
        Streams next = fixture.open();
        next.local.complete().toCompletableFuture().get(2, TimeUnit.SECONDS);
        next.remote.complete().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(next.local.closed(), "Completed streams must retire without poisoning subsequent streams");
        fixture.close();
    }

    @Test
    void pendingWritePreservesDataCompleteOrderAndCancellationReleasesCreditWaiter() throws Exception {
        Fixture fixture = new Fixture(true);
        Streams streams = fixture.open();
        Messages received = new Messages();
        streams.remote.subscribe(received);
        fixture.paused.pauseNextWrite();
        CompletableFuture<Void> first = streams.local.send(new byte[] {1}).toCompletableFuture();
        fixture.paused.entered.get(2, TimeUnit.SECONDS);
        CompletableFuture<Void> second = streams.local.send(new byte[] {2}).toCompletableFuture();
        CompletableFuture<Void> complete = streams.local.complete().toCompletableFuture();
        assertFalse(first.isDone());
        assertFalse(second.isDone(), "A later stream send must await its predecessor");
        assertFalse(complete.isDone(), "COMPLETE must await all preceding stream DATA");
        assertTrue(received.items.isEmpty());
        fixture.paused.resumeWrite();
        CompletableFuture.allOf(first, second, complete).get(2, TimeUnit.SECONDS);
        received.ended.get(2, TimeUnit.SECONDS);
        assertArrayEquals(new byte[] {1}, received.items.poll(2, TimeUnit.SECONDS));
        assertArrayEquals(new byte[] {2}, received.items.poll(2, TimeUnit.SECONDS));
        assertTrue(received.items.isEmpty());
        streams.remote.complete().toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertTrue(streams.local.closed());

        Streams noCredit = fixture.open();
        CompletableFuture<Void> waiting = noCredit.local.send(new byte[] {3}).toCompletableFuture();
        assertFalse(waiting.isDone(), "DATA must wait for peer demand");
        noCredit.local.cancel("cancel blocked send").toCompletableFuture().get(2, TimeUnit.SECONDS);
        assertThrows(java.util.concurrent.ExecutionException.class, () -> waiting.get(2, TimeUnit.SECONDS));
        assertTrue(noCredit.local.closed());
        assertTrue(noCredit.remote.closed());
        fixture.close();
    }

    @Test
    void completedFragmentsReleaseReassemblyBudgetForFollowingItemsAndStreams() throws Exception {
        ProtocolLimits limits = new ProtocolLimits(1024, 512, 64, 128, 256,
            8192, 8192, 16, 16, 16, 1024 * 1024, 1024 * 1024, 64);
        Fixture fixture = new Fixture(false, limits);
        byte[] payload = new byte[8192];
        for (int index = 0; index < payload.length; index++) payload[index] = (byte) index;
        for (int streamIndex = 0; streamIndex < 2; streamIndex++) {
            Streams streams = fixture.open();
            Messages received = new Messages();
            streams.remote.subscribe(received);
            for (int item = 0; item < 3; item++) {
                payload[0] = (byte) (streamIndex * 3 + item);
                streams.local.send(payload).toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertArrayEquals(payload, received.items.poll(2, TimeUnit.SECONDS),
                    "Each completed item must release the entire shared reassembly budget");
            }
            streams.local.complete().toCompletableFuture().get(2, TimeUnit.SECONDS);
            received.ended.get(2, TimeUnit.SECONDS);
            streams.remote.complete().toCompletableFuture().get(2, TimeUnit.SECONDS);
            assertTrue(streams.local.closed());
            assertTrue(streams.remote.closed());
        }
        assertEquals(SessionState.READY, fixture.client.state());
        assertEquals(SessionState.READY, fixture.server.state());
        fixture.close();
    }

    private static void awaitBlockedOn(Thread writer, Object monitor, Thread owner) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(writer.threadId());
            if (info != null && info.getThreadState() == Thread.State.BLOCKED
                && info.getLockOwnerId() == owner.threadId() && info.getLockInfo() != null
                && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(monitor)) return;
            if (writer.getState() == Thread.State.TERMINATED) fail("Writer terminated before the controlled session-lock contention");
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
        }
        fail("Writer did not contend on the expected session monitor");
    }

    @FunctionalInterface private interface Checked { void run() throws Exception; }
    private static Thread daemon(String name, CompletableFuture<Void> result, Checked action) {
        Thread thread = new Thread(() -> {
            try { action.run(); result.complete(null); }
            catch (Throwable error) { result.completeExceptionally(error); }
        }, name);
        thread.setDaemon(true);
        return thread;
    }

    private record Streams(ReactiveStream local, ReactiveStream remote) {}
    private static final class Fixture {
        final ReactiveSession client, server;
        final PausingTransport paused;
        final BlockingQueue<ReactiveStream> incoming = new LinkedBlockingQueue<>();
        Fixture(boolean pause) throws Exception { this(pause, null); }
        Fixture(boolean pause, ProtocolLimits limits) throws Exception {
            MemoryTransport.Pair pair = MemoryTransport.pair();
            paused = pause ? new PausingTransport(pair.left()) : null;
            PrpRuntime runtime = limits == null ? PrpRuntime.defaults() : PrpRuntime.builder().limits(limits).build();
            CompletableFuture<ReactiveSession> accepting = runtime.accept(pair.right()).toCompletableFuture();
            client = runtime.connect(paused == null ? pair.left() : paused).toCompletableFuture().get(2, TimeUnit.SECONDS);
            server = accepting.get(2, TimeUnit.SECONDS);
            server.subscribe(new Flow.Subscriber<>() {
                @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(Long.MAX_VALUE); }
                @Override public void onNext(ReactiveStream stream) { incoming.add(stream); }
                @Override public void onError(Throwable error) { throw new AssertionError(error); }
                @Override public void onComplete() {}
            });
        }
        Streams open() throws Exception {
            ReactiveStream local = client.open(List.of()).toCompletableFuture().get(2, TimeUnit.SECONDS);
            ReactiveStream remote = incoming.poll(2, TimeUnit.SECONDS);
            assertNotNull(remote, "Peer did not receive the real stream OPEN");
            return new Streams(local, remote);
        }
        void close() throws Exception { client.close("test complete").toCompletableFuture().get(2, TimeUnit.SECONDS); }
    }

    private static final class Messages implements Flow.Subscriber<StreamMessage> {
        final BlockingQueue<byte[]> items = new LinkedBlockingQueue<>();
        final CompletableFuture<Void> ended = new CompletableFuture<>();
        @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(4); }
        @Override public void onNext(StreamMessage message) { items.add(message.data()); }
        @Override public void onError(Throwable error) { ended.completeExceptionally(error); }
        @Override public void onComplete() { ended.complete(null); }
    }

    private static final class PausingTransport implements ReactiveTransport {
        final ReactiveTransport delegate;
        final AtomicBoolean pauseNext = new AtomicBoolean();
        volatile CompletableFuture<Void> entered, held;
        volatile TransportLane heldLane;
        volatile byte[] heldFrame;
        PausingTransport(ReactiveTransport delegate) { this.delegate = delegate; }
        @Override public String id() { return delegate.id(); }
        void pauseNextWrite() { entered = new CompletableFuture<>(); held = new CompletableFuture<>(); pauseNext.set(true); }
        void resumeWrite() {
            heldLane.write(heldFrame).whenComplete((ignored, error) -> {
                if (error == null) held.complete(null); else held.completeExceptionally(error);
            });
        }
        @Override public CompletionStage<TransportConnection> connect() {
            return delegate.connect().thenApply(connection -> new TransportConnection() {
                @Override public TransportDescription description() { return connection.description(); }
                @Override public boolean supportsLane(LaneRequirements requirements) { return connection.supportsLane(requirements); }
                @Override public CompletionStage<TransportLane> openLane(LaneRequirements requirements) {
                    return connection.openLane(requirements).thenApply(lane -> new TransportLane() {
                        @Override public String id() { return lane.id(); }
                        @Override public int maxFrameBytes() { return lane.maxFrameBytes(); }
                        @Override public Flow.Publisher<byte[]> incoming() { return lane.incoming(); }
                        @Override public CompletionStage<Void> write(byte[] frame) {
                            if ("reliable".equals(requirements.reliability()) && pauseNext.compareAndSet(true, false)) {
                                heldLane = lane; heldFrame = frame.clone(); entered.complete(null); return held;
                            }
                            return lane.write(frame);
                        }
                        @Override public CompletionStage<Void> close(String reason) { return lane.close(reason); }
                    });
                }
                @Override public CompletionStage<TransportCloseEvent> closed() { return connection.closed(); }
                @Override public CompletionStage<Void> close(Integer code, String reason) { return connection.close(code, reason); }
            });
        }
    }
}
