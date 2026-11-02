package tech.kwik.core.stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import tech.kwik.core.common.EncryptionLevel;
import tech.kwik.core.frame.QuicFrame;
import tech.kwik.core.frame.StreamFrame;
import tech.kwik.core.impl.QuicConnectionImpl;
import tech.kwik.core.impl.Role;
import tech.kwik.core.impl.Version;
import tech.kwik.core.log.NullLogger;

/** Exercises the real stream, bounded send buffer and flow control; only dispatch timing is controlled. */
@Timeout(10)
class SendSchedulingRegressionTest {
    @Test
    void aCallbackBeforeRegistrationReturnsDoesNotStrandConsecutiveWrites() throws Exception {
        try (Fixture fixture = new Fixture()) {
            byte[] first = payload(40_000, 7), second = payload(40_000, 31);
            fixture.output.write(first);
            assertTrue(fixture.buffer.getAvailableBytes() > 0);
            assertTrue(fixture.sender.hasPending(),
                "The first callback must leave remaining bytes scheduled: " + fixture.origin());

            Future<?> write = fixture.writer.submit(() -> { fixture.output.write(second); return null; });
            fixture.awaitBlockedWriter();
            while (fixture.buffer.getAvailableBytes() > fixture.buffer.getMaxSize() - second.length)
                fixture.sender.dispatchOne();
            write.get(2, TimeUnit.SECONDS);
            fixture.sender.drain();

            assertArrayEquals(concat(first, second), fixture.sender.delivered.toByteArray());
            assertEquals(0, fixture.buffer.getAvailableBytes());
            assertTrue(fixture.flow.getFlowControlLimit(fixture.stream) > first.length + second.length,
                "The regression must not rely on peer flow-control exhaustion");
        }
    }

    @Test
    void aWriteLargerThanTheBufferKeepsSendingEveryChunkAfterTheSameCallbackRace() throws Exception {
        try (Fixture fixture = new Fixture()) {
            byte[] data = payload(125_000, 13);
            Future<?> write = fixture.writer.submit(() -> { fixture.output.write(data); return null; });
            fixture.awaitBlockedWriter();
            assertTrue(fixture.sender.hasPending(),
                "A partially buffered large write must retain a send callback: " + fixture.origin());
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            while (!write.isDone()) {
                assertTrue(System.nanoTime() < deadline, "The large write stopped making progress");
                if (!fixture.sender.tryDispatchOne())
                    assertTrue(write.isDone(), "The large write has no pending sender callback");
            }
            write.get(2, TimeUnit.SECONDS);
            fixture.sender.drain();

            assertArrayEquals(data, fixture.sender.delivered.toByteArray());
            assertEquals(0, fixture.buffer.getAvailableBytes());
            assertFalse(fixture.sender.hasPending());
        }
    }

    private static byte[] payload(int length, int seed) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) data[i] = (byte) (i * 17 + seed);
        return data;
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private static final class Fixture implements AutoCloseable {
        final ControlledSender sender = new ControlledSender();
        final ExecutorService writer = Executors.newSingleThreadExecutor();
        final FlowControl flow = new FlowControl(Role.Server, 1_000_000, 1_000_000, 1_000_000, 1_000_000);
        final QuicStreamImpl stream;
        final StreamOutputStreamImpl output;
        final SendBuffer buffer;

        @SuppressWarnings("unchecked")
        Fixture() throws Exception {
            QuicConnectionImpl connection = mock(QuicConnectionImpl.class);
            doAnswer(call -> { sender.register(call.getArgument(0)); return null; })
                .when(connection).send(any(Function.class), anyInt(), any(EncryptionLevel.class),
                    any(Consumer.class), anyBoolean());
            stream = new QuicStreamImpl(Version.getDefault(), 4, Role.Server,
                connection, mock(StreamManager.class), flow, new NullLogger());
            output = (StreamOutputStreamImpl) stream.getOutputStream();
            buffer = (SendBuffer) field(output, "sendBuffer");
        }

        void awaitBlockedWriter() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
            while (field(buffer, "blockingWriterThread") == null && System.nanoTime() < deadline)
                Thread.sleep(1);
            assertNotNull(field(buffer, "blockingWriterThread"),
                "The real bounded send buffer must require its sender to make progress");
        }

        String origin() {
            return StreamOutputStreamImpl.class.getProtectionDomain().getCodeSource().getLocation().toString();
        }

        @Override public void close() throws InterruptedException {
            try {
                writer.shutdownNow();
                assertTrue(writer.awaitTermination(2, TimeUnit.SECONDS));
            } finally {
                sender.close();
            }
        }
    }

    private static final class ControlledSender implements AutoCloseable {
        final BlockingQueue<Function<Integer, QuicFrame>> pending = new LinkedBlockingQueue<>();
        final ExecutorService thread = Executors.newSingleThreadExecutor();
        final AtomicBoolean firstRegistration = new AtomicBoolean(true);
        final ByteArrayOutputStream delivered = new ByteArrayOutputStream();

        void register(Function<Integer, QuicFrame> callback) throws Exception {
            pending.add(callback);
            if (firstRegistration.compareAndSet(true, false)) {
                // The real sender can consume a newly queued request before connection.send returns.
                // Force that valid ordering once, without changing any Kwik implementation code.
                thread.submit(() -> { dispatchOne(); return null; }).get(2, TimeUnit.SECONDS);
            }
        }

        boolean hasPending() { return !pending.isEmpty(); }

        void dispatchOne() throws Exception {
            assertTrue(tryDispatchOne(), "Buffered bytes have no queued sender callback");
        }

        boolean tryDispatchOne() throws Exception {
            Function<Integer, QuicFrame> callback = pending.poll(250, TimeUnit.MILLISECONDS);
            if (callback == null) return false;
            QuicFrame frame = callback.apply(1200);
            if (frame instanceof StreamFrame data) {
                assertEquals(delivered.size(), data.getOffset(), "Stream byte offsets must remain ordered");
                delivered.writeBytes(data.getStreamData());
            }
            return true;
        }

        void drain() throws Exception {
            while (hasPending()) dispatchOne();
        }

        @Override public void close() throws InterruptedException {
            thread.shutdownNow();
            assertTrue(thread.awaitTermination(2, TimeUnit.SECONDS));
        }
    }

    private static Object field(Object value, String name) throws ReflectiveOperationException {
        Field field = value.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(value);
    }
}
