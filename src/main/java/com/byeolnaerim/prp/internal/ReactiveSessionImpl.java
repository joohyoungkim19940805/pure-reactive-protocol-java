package com.byeolnaerim.prp.internal;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.DatagramAcceptor;
import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.ReactiveStream;
import com.byeolnaerim.prp.SessionOrigin;
import com.byeolnaerim.prp.SessionDatagram;
import com.byeolnaerim.prp.SessionSignal;
import com.byeolnaerim.prp.SessionState;
import com.byeolnaerim.prp.SignalAcceptor;
import com.byeolnaerim.prp.StreamAcceptor;
import com.byeolnaerim.prp.StreamMessage;
import com.byeolnaerim.prp.core.Capabilities;
import com.byeolnaerim.prp.core.CapabilityDescriptor;
import com.byeolnaerim.prp.core.CapabilitySet;
import com.byeolnaerim.prp.core.DatagramCodec;
import com.byeolnaerim.prp.core.FrameCodec;
import com.byeolnaerim.prp.core.FrameKind;
import com.byeolnaerim.prp.core.LivenessCodec;
import com.byeolnaerim.prp.core.NegotiatedCapability;
import com.byeolnaerim.prp.core.PeerProtocolLimits;
import com.byeolnaerim.prp.core.ProtocolExtension;
import com.byeolnaerim.prp.core.ProtocolFrame;
import com.byeolnaerim.prp.core.ProtocolLimits;
import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.core.PrpText;
import com.byeolnaerim.prp.error.CapabilityMismatchException;
import com.byeolnaerim.prp.error.ConnectionLostException;
import com.byeolnaerim.prp.error.LivenessTimeoutException;
import com.byeolnaerim.prp.error.PrpException;
import com.byeolnaerim.prp.error.ProtocolViolationException;
import com.byeolnaerim.prp.error.StreamClosedException;
import com.byeolnaerim.prp.transport.LaneRequirements;
import com.byeolnaerim.prp.transport.ReactiveTransport;
import com.byeolnaerim.prp.transport.TransportConnection;
import com.byeolnaerim.prp.transport.TransportLane;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class ReactiveSessionImpl implements ReactiveSession, SessionInternals {
    private static final BigInteger TWO = BigInteger.valueOf(2);
    private static final BigInteger MAX_U64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final long MAX_CREDIT = 0xffffffffL;
    private static final String SESSION_ID = "prp.session.id";
    private static final String ERROR_CODE = "prp.error.code";
    private static final int MAX_ERROR_REASON_BYTES = 1024;
    private static final ScheduledExecutorService TIMER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "prp-java-timer");
        thread.setDaemon(true);
        return thread;
    });

    private final PrpRuntime runtime;
    private final BoundedPublisher<ReactiveStream> incomingStreams;
    private final BoundedPublisher<SessionSignal> incomingSignals;
    private final BoundedPublisher<SessionDatagram> incomingDatagrams;
    private final Map<BigInteger, StreamState> streams = new LinkedHashMap<>();
    private final LinkedHashMap<BigInteger, Boolean> retiredStreams = new LinkedHashMap<>();
    private final List<StreamAcceptor> acceptors = new CopyOnWriteArrayList<>();
    private final List<SignalAcceptor> signalAcceptors = new CopyOnWriteArrayList<>();
    private final List<DatagramAcceptor> datagramAcceptors = new CopyOnWriteArrayList<>();
    private final List<Consumer<SessionState>> stateListeners = new CopyOnWriteArrayList<>();
    private final List<AutoCloseable> extensionDisposers = new CopyOnWriteArrayList<>();
    private final Map<Class<?>, Object> attachments = new ConcurrentHashMap<>();
    private final CompletableFuture<ReactiveSessionImpl> ready = new CompletableFuture<>();
    private volatile SessionState state = SessionState.IDLE;
    private volatile String sessionId = UUID.randomUUID().toString();
    private volatile SessionOrigin origin;
    private volatile TransportConnection connection;
    private volatile TransportLane lane;
    private volatile TransportLane datagramLane;
    private volatile List<CapabilityDescriptor> offeredCapabilities = List.of();
    private volatile int remoteMaxInboundDatagramBytes;
    private volatile CapabilitySet negotiated = new CapabilitySet(List.of());
    private volatile ProtocolLimits outboundLimits;
    private volatile int remoteMaxInboundStreams = Integer.MAX_VALUE;
    private volatile int remoteMaxInboundItemBytes = Integer.MAX_VALUE;
    private BigInteger nextStreamId = BigInteger.ZERO;
    private BigInteger highestRemoteStreamId = BigInteger.ZERO;
    private BigInteger outgoingSequence = BigInteger.ONE;
    private BigInteger expectedIncomingSequence = BigInteger.ONE;
    private CompletableFuture<Void> writeTail = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> openTail = CompletableFuture.completedFuture(null);
    private int localActiveStreams;
    private int remoteActiveStreams;
    private int retainedStreamAttributeBytes;
    private int inFlightReassemblyBytes;
    private int pendingSignalBytes;
    private int activeSignalTasks;
    private volatile long lastInboundNanos = System.nanoTime();
    private BigInteger nextProbeId = BigInteger.ONE;
    private volatile java.util.concurrent.ScheduledFuture<?> livenessTask;
    private volatile boolean closing;

    public ReactiveSessionImpl(PrpRuntime runtime) {
        this.runtime = java.util.Objects.requireNonNull(runtime);
        this.outboundLimits = runtime.limits();
        this.incomingStreams = new BoundedPublisher<>(runtime.limits().maxPendingIncomingStreams());
        this.incomingSignals = new BoundedPublisher<>(runtime.limits().maxPendingSignals());
        this.incomingDatagrams = new BoundedPublisher<>(runtime.datagrams() == null ? 1 : runtime.datagrams().maxPending());
        this.acceptors.addAll(runtime.streamAcceptors());
        this.signalAcceptors.addAll(runtime.signalAcceptors());
    }

    public CompletionStage<ReactiveSessionImpl> attach(ReactiveTransport transport, SessionOrigin origin) {
        synchronized (this) {
            if (state == SessionState.CLOSED) return Stages.failed(new PrpException("Session is closed.", "SESSION_CLOSED"));
            if (state != SessionState.IDLE) return Stages.failed(new PrpException("This alpha does not reattach a detached logical session.", "SESSION_REATTACH_UNSUPPORTED"));
            this.origin = origin;
            this.nextStreamId = origin == SessionOrigin.INITIATOR ? BigInteger.ONE : TWO;
            transition(SessionState.CONNECTING);
        }

        CompletionStage<TransportConnection> connectionStage;
        try { connectionStage = transport.connect(); }
        catch (Throwable error) { detach(error); return Stages.failed(error); }

        connectionStage.thenCompose(nextConnection -> {
            connection = nextConnection;
            nextConnection.closed().whenComplete((event, error) -> {
                if (state == SessionState.CLOSED || closing) return;
                detach(error == null ? new ConnectionLostException("Physical transport closed: " + (event == null ? "closed" : event.reason())) : Stages.unwrap(error));
            });
            return nextConnection.openLane(LaneRequirements.reliableOrdered(Math.max(runtime.limits().maxFrameBytes(), ProtocolLimits.BOOTSTRAP.maxFrameBytes())));
        }).thenCompose(nextLane -> {
            lane = nextLane;
            subscribeIncoming(nextLane);
            return openOptionalDatagramLane();
        }).whenComplete((ignored, error) -> {
            if (error != null) {
                Throwable failure = Stages.unwrap(error);
                detach(failure);
                ready.completeExceptionally(failure);
                return;
            }
            if (origin == SessionOrigin.INITIATOR) {
                List<ProtocolAttribute> hello = new ArrayList<>();
                hello.add(ProtocolAttribute.requiredText(SESSION_ID, sessionId));
                hello.addAll(Capabilities.toAttributes(offeredCapabilities, runtime.policy()));
                writeFrame(FrameKind.HELLO, BigInteger.ZERO, 0, 0, hello, new byte[0], true).whenComplete((ignoredWrite, writeError) -> {
                    if (writeError != null) {
                        detach(Stages.unwrap(writeError));
                        ready.completeExceptionally(Stages.unwrap(writeError));
                    }
                });
            }
        });

        TIMER.schedule(() -> {
            if (ready.isDone()) return;
            PrpException error = new PrpException("PRP session negotiation exceeded " + runtime.handshakeTimeout().toMillis() + "ms.", "HANDSHAKE_TIMEOUT");
            ready.completeExceptionally(error);
            detach(error);
        }, runtime.handshakeTimeout().toMillis(), TimeUnit.MILLISECONDS);

        return ready;
    }

    @Override public SessionState state() { return state; }
    @Override public String sessionId() { return sessionId; }
    @Override public Flow.Publisher<SessionSignal> signals() { return incomingSignals; }
    @Override public Flow.Publisher<SessionDatagram> datagrams() { return incomingDatagrams; }
    @Override public int maxDatagramBytes() {
        TransportLane current = datagramLane;
        if (current == null || remoteMaxInboundDatagramBytes <= 0 || !negotiated.has(DatagramCodec.CAPABILITY_ID)) return 0;
        return Math.max(0, Math.min(remoteMaxInboundDatagramBytes, current.maxFrameBytes() - DatagramCodec.HEADER_BYTES));
    }
    @Override public boolean supports(String capabilityId) { return negotiated.has(capabilityId); }

    @Override
    public AutoCloseable onStateChange(Consumer<SessionState> listener) {
        stateListeners.add(listener);
        return () -> stateListeners.remove(listener);
    }

    @Override public void subscribe(Flow.Subscriber<? super ReactiveStream> subscriber) { incomingStreams.subscribe(subscriber); }

    @Override
    public CompletionStage<ReactiveStream> open(List<ProtocolAttribute> attributes) {
        List<ProtocolAttribute> snapshot = snapshot(attributes);
        CompletableFuture<ReactiveStream> result = new CompletableFuture<>();
        synchronized (this) {
            openTail = openTail.handle((ignored, error) -> null).thenCompose(ignored -> openNext(snapshot).handle((stream, error) -> {
                if (error != null) result.completeExceptionally(Stages.unwrap(error));
                else result.complete(stream);
                return (Void) null;
            })).toCompletableFuture();
        }
        return result;
    }

    private CompletionStage<ReactiveStream> openNext(List<ProtocolAttribute> attributes) {
        synchronized (this) {
            assertReady();
            if (nextStreamId.compareTo(MAX_U64) > 0) return Stages.failed(new PrpException("Logical stream id space is exhausted.", "STREAM_ID_EXHAUSTED"));
            if (localActiveStreams >= remoteMaxInboundStreams) return Stages.failed(new PrpException("Peer inbound stream limit reached.", "RESOURCE_EXHAUSTED"));
            BigInteger streamId = nextStreamId;
            StreamState streamState = createStreamState(streamId, attributes, true);
            if ((long) retainedStreamAttributeBytes + streamState.retainedAttributeBytes > runtime.limits().maxRetainedStreamAttributeBytes()) return Stages.failed(new PrpException("Retained stream attribute budget reached.", "RESOURCE_EXHAUSTED"));
            streams.put(streamId, streamState);
            localActiveStreams += 1;
            retainedStreamAttributeBytes += streamState.retainedAttributeBytes;
            ReactiveStream stream = new ReactiveStreamImpl(streamState);
            return writeFrame(FrameKind.OPEN, streamId, 0, 0, attributes, new byte[0], false).handle((ignored, error) -> {
                synchronized (ReactiveSessionImpl.this) {
                    if (error != null) {
                        releaseState(streamState, true);
                        throw new java.util.concurrent.CompletionException(Stages.unwrap(error));
                    }
                    nextStreamId = nextStreamId.add(TWO);
                }
                return stream;
            });
        }
    }

    @Override
    public CompletionStage<Void> signal(List<ProtocolAttribute> attributes, byte[] payload) {
        try { assertReady(); }
        catch (Throwable error) { return Stages.failed(error); }
        return writeFrame(FrameKind.SIGNAL, BigInteger.ZERO, 0, 0, snapshot(attributes), payload == null ? new byte[0] : payload.clone(), false);
    }

    @Override
    public CompletionStage<Void> sendDatagram(byte[] data) {
        try { assertReady(); }
        catch (Throwable error) { return Stages.failed(error); }
        if (data == null) return Stages.failed(new NullPointerException("data"));
        TransportLane current = datagramLane;
        int maxPayloadBytes = maxDatagramBytes();
        if (current == null || maxPayloadBytes <= 0 || !negotiated.has(DatagramCodec.CAPABILITY_ID)) {
            return Stages.failed(new PrpException("Native PRP datagrams were not negotiated on this transport.", "DATAGRAM_UNAVAILABLE"));
        }
        if (data.length > maxPayloadBytes) {
            return Stages.failed(new IllegalArgumentException("PRP native datagram exceeds the negotiated/carrier limit of " + maxPayloadBytes + " application bytes."));
        }
        byte[] frame;
        try { frame = DatagramCodec.encode(data.clone(), current.maxFrameBytes()); }
        catch (Throwable error) { return Stages.failed(error); }
        try {
            return current.write(frame).whenComplete((ignored, error) -> {
                if (error != null) disableDatagramLane(current);
            });
        } catch (Throwable error) {
            disableDatagramLane(current);
            return Stages.failed(error);
        }
    }

    @Override
    public CompletionStage<Void> close(String reason) {
        synchronized (this) {
            if (state == SessionState.CLOSED) return Stages.completed();
            closing = true;
        }
        byte[] payload = reason == null ? new byte[0] : boundedUtf8(reason);
        return writeTerminal(FrameKind.CLOSE, BigInteger.ZERO, List.of(), payload).handle((ignored, error) -> null).thenCompose(ignored -> finishClosed(reason == null ? "Session closed." : reason));
    }

    @Override
    public AutoCloseable addStreamAcceptor(StreamAcceptor acceptor) {
        acceptors.add(acceptor);
        return () -> acceptors.remove(acceptor);
    }

    @Override
    public AutoCloseable addSignalAcceptor(SignalAcceptor acceptor) {
        signalAcceptors.add(acceptor);
        return () -> signalAcceptors.remove(acceptor);
    }

    @Override
    public AutoCloseable addDatagramAcceptor(DatagramAcceptor acceptor) {
        datagramAcceptors.add(acceptor);
        return () -> datagramAcceptors.remove(acceptor);
    }

    @Override public CapabilitySet capabilities() { return negotiated; }
    @Override public <T> void attachment(Class<T> type, T value) { if (value == null) attachments.remove(type); else attachments.put(type, value); }
    @Override public <T> T attachment(Class<T> type) { return type.cast(attachments.get(type)); }

    private CompletionStage<Void> openOptionalDatagramLane() {
        TransportConnection currentConnection = connection;
        if (currentConnection == null) return Stages.failed(new ConnectionLostException());
        if (runtime.datagrams() == null) {
            offeredCapabilities = availableCapabilities(false);
            return Stages.completed();
        }
        LaneRequirements requirements = LaneRequirements.bestEffortUnordered(runtime.datagrams().maxInboundBytes() + DatagramCodec.HEADER_BYTES);
        if (!currentConnection.supportsLane(requirements)) {
            offeredCapabilities = availableCapabilities(false);
            if (runtime.policy().require().contains(DatagramCodec.CAPABILITY_ID)) {
                return Stages.failed(new CapabilityMismatchException("The selected transport does not expose a best-effort/unordered datagram lane required by prp.core.datagram."));
            }
            return Stages.completed();
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            currentConnection.openLane(requirements).whenComplete((nextLane, error) -> {
                if (error != null) {
                    offeredCapabilities = availableCapabilities(false);
                    if (runtime.policy().require().contains(DatagramCodec.CAPABILITY_ID)) result.completeExceptionally(Stages.unwrap(error));
                    else result.complete(null);
                    return;
                }
                datagramLane = nextLane;
                offeredCapabilities = availableCapabilities(true);
                result.complete(null);
            });
        } catch (Throwable error) {
            offeredCapabilities = availableCapabilities(false);
            if (runtime.policy().require().contains(DatagramCodec.CAPABILITY_ID)) result.completeExceptionally(error);
            else result.complete(null);
        }
        return result;
    }

    private List<CapabilityDescriptor> availableCapabilities(boolean datagramAvailable) {
        java.util.LinkedHashSet<String> available = new java.util.LinkedHashSet<>();
        for (CapabilityDescriptor capability : runtime.capabilities()) {
            if (datagramAvailable || !DatagramCodec.CAPABILITY_ID.equals(capability.id())) available.add(capability.id());
        }
        boolean changed;
        do {
            changed = false;
            for (ProtocolExtension extension : runtime.extensions()) {
                if (!available.contains(extension.capability().id())) continue;
                if (!available.containsAll(extension.requiredCapabilities())) {
                    available.remove(extension.capability().id());
                    changed = true;
                }
            }
        } while (changed);
        return runtime.capabilities().stream().filter(capability -> available.contains(capability.id())).toList();
    }

    private void subscribeIncoming(TransportLane transportLane) {
        transportLane.incoming().subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;
            @Override public void onSubscribe(Flow.Subscription subscription) { this.subscription = subscription; subscription.request(1); }
            @Override public void onNext(byte[] item) {
                handleRaw(item).whenComplete((ignored, error) -> {
                    if (error != null) detach(Stages.unwrap(error));
                    else if (state != SessionState.CLOSED && state != SessionState.DETACHED) subscription.request(1);
                });
            }
            @Override public void onError(Throwable throwable) { detach(throwable); }
            @Override public void onComplete() { if (state != SessionState.CLOSED && !closing) detach(new ConnectionLostException()); }
        });
    }

    private CompletionStage<Void> handleRaw(byte[] raw) {
        try {
            ProtocolFrame frame;
            synchronized (this) {
                frame = FrameCodec.decode(raw, state == SessionState.CONNECTING ? ProtocolLimits.BOOTSTRAP : runtime.limits());
                if (!frame.sequence().equals(expectedIncomingSequence)) throw new ProtocolViolationException("Expected peer sequence " + expectedIncomingSequence + ", received " + frame.sequence() + ".");
                expectedIncomingSequence = expectedIncomingSequence.add(BigInteger.ONE);
                lastInboundNanos = System.nanoTime();
            }
            return handleFrame(frame);
        } catch (Throwable error) {
            return Stages.failed(error);
        }
    }

    private CompletionStage<Void> handleFrame(ProtocolFrame frame) {
        if (frame.kind() == FrameKind.HELLO) return handleHello(frame);
        if (frame.kind() == FrameKind.WELCOME) return handleWelcome(frame);
        if (frame.kind() == FrameKind.ERROR && frame.streamId().signum() == 0) return Stages.failed(remoteError(frame, "Remote session error.", "REMOTE_ERROR"));
        synchronized (this) { if (state != SessionState.READY) return Stages.failed(new ProtocolViolationException(frame.kind() + " arrived before session negotiation completed.")); }
        return switch (frame.kind()) {
            case PING -> handlePing(frame);
            case PONG -> handlePong(frame);
            case CLOSE -> finishClosed(remoteReason(frame, "Remote closed the session."));
            case OPEN -> handleOpen(frame);
            case SIGNAL -> handleSignal(frame);
            case DATA, FRAGMENT, DEMAND, COMPLETE, CANCEL, ERROR -> handleStreamFrame(frame);
            case HELLO, WELCOME -> Stages.failed(new ProtocolViolationException("Unexpected negotiation frame."));
        };
    }

    private CompletionStage<Void> handleHello(ProtocolFrame frame) {
        synchronized (this) {
            if (origin != SessionOrigin.ACCEPTOR || state != SessionState.CONNECTING) return Stages.failed(new ProtocolViolationException("Unexpected HELLO frame."));
        }
        try {
            String remoteSessionId = validateHandshake(frame.attributes(), "HELLO");
            CapabilitySet nextNegotiated = Capabilities.negotiate(offeredCapabilities, Capabilities.fromAttributes(frame.attributes()), runtime.policy());
            synchronized (this) {
                sessionId = remoteSessionId;
                negotiated = nextNegotiated;
                applyPeerLimits();
                applyDatagramCapability();
            }
            return attachExtensions().thenCompose(ignored -> {
                List<CapabilityDescriptor> responseCapabilities = new ArrayList<>();
                for (NegotiatedCapability capability : negotiated) responseCapabilities.add(new CapabilityDescriptor(capability.id(), capability.version(), capability.version(), capability.local().parameters()));
                List<ProtocolAttribute> attributes = new ArrayList<>();
                attributes.add(ProtocolAttribute.requiredText(SESSION_ID, sessionId));
                attributes.addAll(Capabilities.toAttributes(responseCapabilities, runtime.policy()));
                return writeFrame(FrameKind.WELCOME, BigInteger.ZERO, 0, 0, attributes, new byte[0], true);
            }).thenRun(() -> {
                synchronized (ReactiveSessionImpl.this) { transition(SessionState.READY); }
                startLiveness();
                ready.complete(this);
            }).exceptionally(error -> { handshakeFailed(Stages.unwrap(error)); return null; });
        } catch (Throwable error) {
            handshakeFailed(error);
            return Stages.failed(error);
        }
    }

    private CompletionStage<Void> handleWelcome(ProtocolFrame frame) {
        synchronized (this) {
            if (origin != SessionOrigin.INITIATOR || state != SessionState.CONNECTING) return Stages.failed(new ProtocolViolationException("Unexpected WELCOME frame."));
        }
        try {
            String echoed = validateHandshake(frame.attributes(), "WELCOME");
            if (!sessionId.equals(echoed)) throw new ProtocolViolationException("WELCOME session id does not match HELLO.");
            CapabilitySet nextNegotiated = Capabilities.negotiate(offeredCapabilities, Capabilities.fromAttributes(frame.attributes()), runtime.policy());
            synchronized (this) {
                negotiated = nextNegotiated;
                for (String required : runtime.policy().require()) if (!negotiated.has(required)) throw new CapabilityMismatchException("Required capability was not negotiated: " + required);
                applyPeerLimits();
                applyDatagramCapability();
            }
            return attachExtensions().thenRun(() -> {
                synchronized (ReactiveSessionImpl.this) { transition(SessionState.READY); }
                startLiveness();
                ready.complete(this);
            }).exceptionally(error -> { handshakeFailed(Stages.unwrap(error)); return null; });
        } catch (Throwable error) {
            handshakeFailed(error);
            return Stages.failed(error);
        }
    }

    private void handshakeFailed(Throwable error) {
        ready.completeExceptionally(error);
        writeSessionError(error).whenComplete((ignored, writeError) -> detach(error));
    }

    private String validateHandshake(List<ProtocolAttribute> attributes, String frameName) {
        List<ProtocolAttribute> sessionIds = attributes.stream().filter(attribute -> SESSION_ID.equals(attribute.id())).toList();
        if (sessionIds.size() != 1 || !sessionIds.getFirst().required()) throw new ProtocolViolationException(frameName + " must contain exactly one required " + SESSION_ID + ".");
        for (ProtocolAttribute attribute : attributes) {
            if (!attribute.required()) continue;
            if (SESSION_ID.equals(attribute.id()) || attribute.id().startsWith(Capabilities.PREFIX)) continue;
            throw new CapabilityMismatchException("Unknown required handshake attribute: " + attribute.id());
        }
        String value = PrpText.strictText(sessionIds.getFirst().value());
        if (value.isEmpty()) throw new ProtocolViolationException(frameName + " is missing a non-empty " + SESSION_ID + ".");
        return value;
    }

    private void applyPeerLimits() {
        NegotiatedCapability limitsCapability = negotiated.get(Capabilities.LIMITS);
        if (limitsCapability == null) throw new CapabilityMismatchException("Required capability was not negotiated: " + Capabilities.LIMITS);
        PeerProtocolLimits remote = Capabilities.decodePeerLimits(limitsCapability.remote().parameters());
        remoteMaxInboundStreams = remote.maxInboundStreams();
        remoteMaxInboundItemBytes = remote.maxInboundItemBytes();
        outboundLimits = runtime.limits().outboundFor(remote);
        NegotiatedCapability liveness = negotiated.get(Capabilities.LIVENESS);
        if (liveness == null) throw new CapabilityMismatchException("Required capability was not negotiated: " + Capabilities.LIVENESS);
        LivenessCodec.decodeParameters(liveness.remote().parameters());
    }

    private void applyDatagramCapability() {
        NegotiatedCapability capability = negotiated.get(DatagramCodec.CAPABILITY_ID);
        if (capability == null) {
            TransportLane current = datagramLane;
            datagramLane = null;
            remoteMaxInboundDatagramBytes = 0;
            if (current != null) current.close("prp.core.datagram not negotiated");
            return;
        }
        if (runtime.datagrams() == null || datagramLane == null) {
            throw new CapabilityMismatchException("prp.core.datagram was negotiated without an available native datagram lane.");
        }
        remoteMaxInboundDatagramBytes = DatagramCodec.decodeCapability(capability.remote().parameters());
        subscribeIncomingDatagrams(datagramLane);
    }

    private void subscribeIncomingDatagrams(TransportLane transportLane) {
        transportLane.incoming().subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;
            @Override public void onSubscribe(Flow.Subscription subscription) { this.subscription = subscription; subscription.request(1); }
            @Override public void onNext(byte[] frame) {
                if (datagramLane != transportLane || state == SessionState.CLOSED || state == SessionState.DETACHED) return;
                try {
                    byte[] data = DatagramCodec.decode(frame, runtime.datagrams().maxInboundBytes());
                    lastInboundNanos = System.nanoTime();
                    SessionDatagram datagram = new SessionDatagram(data);
                    boolean handled = false;
                    for (DatagramAcceptor acceptor : datagramAcceptors) {
                        boolean accepted;
                        try { accepted = acceptor.accepts(ReactiveSessionImpl.this, datagram); }
                        catch (Throwable ignored) { accepted = false; }
                        if (!accepted) continue;
                        handled = true;
                        try {
                            CompletionStage<Void> stage = acceptor.handle(ReactiveSessionImpl.this, datagram);
                            if (stage != null) stage.exceptionally(error -> null);
                        } catch (Throwable ignored) { }
                        break;
                    }
                    // Best-effort semantics: local pressure drops the newest unmatched datagram instead of poisoning the session.
                    if (!handled) incomingDatagrams.emit(datagram);
                } catch (Throwable ignored) {
                    // Malformed datagrams are isolated from the reliable PRP session.
                } finally {
                    if (datagramLane == transportLane && state != SessionState.CLOSED && state != SessionState.DETACHED) subscription.request(1);
                }
            }
            @Override public void onError(Throwable throwable) { disableDatagramLane(transportLane); }
            @Override public void onComplete() { disableDatagramLane(transportLane); }
        });
    }

    private synchronized void disableDatagramLane(TransportLane transportLane) {
        if (datagramLane != transportLane) return;
        datagramLane = null;
        remoteMaxInboundDatagramBytes = 0;
        incomingDatagrams.complete();
    }

    private CompletionStage<Void> attachExtensions() {
        CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
        for (ProtocolExtension extension : runtime.extensions()) {
            if (!negotiated.has(extension.capability().id())) continue;
            tail = tail.thenCompose(ignored -> extension.attach(this).thenAccept(disposer -> {
                if (disposer != null) extensionDisposers.add(disposer);
            })).toCompletableFuture();
        }
        return tail;
    }

    private CompletionStage<Void> handleOpen(ProtocolFrame frame) {
        ReactiveStream stream;
        StreamState streamState;
        synchronized (this) {
            BigInteger expected = highestRemoteStreamId.signum() == 0
                ? (origin == SessionOrigin.INITIATOR ? TWO : BigInteger.ONE)
                : highestRemoteStreamId.add(TWO);
            if (!frame.streamId().equals(expected)) return Stages.failed(new ProtocolViolationException("Peer must allocate stream ids contiguously by parity; expected " + expected + ", received " + frame.streamId() + "."));
            highestRemoteStreamId = frame.streamId();
            if (remoteActiveStreams >= runtime.limits().maxInboundStreams()) return rejectRemoteOpen(frame.streamId(), new PrpException("Inbound stream limit reached.", "RESOURCE_EXHAUSTED"));
            streamState = createStreamState(frame.streamId(), frame.attributes(), false);
            if ((long) retainedStreamAttributeBytes + streamState.retainedAttributeBytes > runtime.limits().maxRetainedStreamAttributeBytes()) return rejectRemoteOpen(frame.streamId(), new PrpException("Retained stream attribute budget reached.", "RESOURCE_EXHAUSTED"));
            stream = new ReactiveStreamImpl(streamState);
        }
        for (StreamAcceptor acceptor : acceptors) {
            boolean accepted;
            try { accepted = acceptor.accepts(this, stream); }
            catch (Throwable error) { return stream.fail(error); }
            if (!accepted) continue;
            synchronized (this) {
                streams.put(frame.streamId(), streamState);
                remoteActiveStreams += 1;
                retainedStreamAttributeBytes += streamState.retainedAttributeBytes;
            }
            try {
                CompletionStage<Void> handled = acceptor.handle(this, stream);
                handled.whenComplete((ignored, error) -> { if (error != null && !stream.closed()) stream.fail(Stages.unwrap(error)); });
            } catch (Throwable error) {
                stream.fail(error);
            }
            return Stages.completed();
        }
        synchronized (this) {
            if (incomingStreams.size() >= runtime.limits().maxPendingIncomingStreams()) return rejectRemoteOpen(frame.streamId(), new PrpException("Pending incoming stream limit reached.", "RESOURCE_EXHAUSTED"));
            streams.put(frame.streamId(), streamState);
            remoteActiveStreams += 1;
            retainedStreamAttributeBytes += streamState.retainedAttributeBytes;
        }
        if (!incomingStreams.emit(stream)) return rejectRemoteOpen(frame.streamId(), new PrpException("Pending incoming stream limit reached.", "RESOURCE_EXHAUSTED"));
        return Stages.completed();
    }

    private CompletionStage<Void> rejectRemoteOpen(BigInteger streamId, PrpException error) {
        synchronized (this) { retire(streamId, false); }
        return writeTerminal(FrameKind.ERROR, streamId, List.of(ProtocolAttribute.text(ERROR_CODE, error.code())), boundedUtf8(error.getMessage()));
    }

    private CompletionStage<Void> handleSignal(ProtocolFrame frame) {
        SessionSignal signal = new SessionSignal(frame.payload(), frame.attributes());
        int bytes = signal.data().length + attributeMemoryBytes(signal.attributes());
        synchronized (this) {
            if (bytes > runtime.limits().maxInFlightSignalBytes() || pendingSignalBytes + bytes > runtime.limits().maxInFlightSignalBytes()) return Stages.failed(new ProtocolViolationException("Session signal byte budget exceeded."));
        }
        for (SignalAcceptor acceptor : signalAcceptors) {
            boolean accepted;
            try { accepted = acceptor.accepts(this, signal); }
            catch (Throwable error) { return Stages.failed(error); }
            if (!accepted) continue;
            synchronized (this) {
                if (activeSignalTasks >= runtime.limits().maxPendingSignals()) return Stages.failed(new ProtocolViolationException("Too many in-flight session signal handlers."));
                activeSignalTasks += 1;
                pendingSignalBytes += bytes;
            }
            CompletionStage<Void> handled;
            try { handled = acceptor.handle(this, signal); }
            catch (Throwable error) { handled = Stages.failed(error); }
            handled.whenComplete((ignored, error) -> {
                synchronized (ReactiveSessionImpl.this) {
                    activeSignalTasks = Math.max(0, activeSignalTasks - 1);
                    pendingSignalBytes = Math.max(0, pendingSignalBytes - bytes);
                }
                if (error != null) detach(Stages.unwrap(error));
            });
            return Stages.completed();
        }
        synchronized (this) {
            if (incomingSignals.size() >= runtime.limits().maxPendingSignals()) return Stages.failed(new ProtocolViolationException("Pending session signal limit reached."));
            pendingSignalBytes += bytes;
        }
        if (!incomingSignals.emit(signal)) return Stages.failed(new ProtocolViolationException("Pending session signal limit reached."));
        return Stages.completed();
    }

    private CompletionStage<Void> handlePing(ProtocolFrame frame) {
        try { LivenessCodec.decodeProbe(frame.payload()); }
        catch (Throwable error) { return Stages.failed(error); }
        return writeFrame(FrameKind.PONG, BigInteger.ZERO, 0, 0, List.of(), frame.payload(), false);
    }

    private CompletionStage<Void> handlePong(ProtocolFrame frame) {
        try { LivenessCodec.decodeProbe(frame.payload()); return Stages.completed(); }
        catch (Throwable error) { return Stages.failed(error); }
    }

    private CompletionStage<Void> handleStreamFrame(ProtocolFrame frame) {
        StreamState stream;
        synchronized (this) {
            stream = streams.get(frame.streamId());
            if (stream == null) {
                Boolean retired = retiredStreams.get(frame.streamId());
                boolean past = isPastStreamId(frame.streamId());
                if (past && (frame.kind() == FrameKind.DEMAND || frame.kind() == FrameKind.COMPLETE || frame.kind() == FrameKind.CANCEL || frame.kind() == FrameKind.ERROR)) return Stages.completed();
                if ((frame.kind() == FrameKind.DATA || frame.kind() == FrameKind.FRAGMENT) && (Boolean.TRUE.equals(retired) || (retired == null && past))) return Stages.completed();
                return Stages.failed(new ProtocolViolationException("Frame references unknown stream " + frame.streamId() + "."));
            }
        }
        return switch (frame.kind()) {
            case DATA -> handleData(stream, frame);
            case FRAGMENT -> handleFragment(stream, frame);
            case DEMAND -> handleDemand(stream, frame.credit());
            case COMPLETE -> handleComplete(stream);
            case CANCEL -> { cancelStream(stream, remoteError(frame, "Remote cancelled the stream.", "REMOTE_CANCELLED"), false); yield Stages.completed(); }
            case ERROR -> { cancelStream(stream, remoteError(frame, "Remote stream error.", "REMOTE_ERROR"), false); yield Stages.completed(); }
            default -> Stages.failed(new ProtocolViolationException("Unexpected stream frame kind."));
        };
    }

    private CompletionStage<Void> handleData(StreamState stream, ProtocolFrame frame) {
        synchronized (stream) {
            if (stream.inboundClosed || stream.cancelled) return Stages.failed(new ProtocolViolationException("DATA received for closed stream " + frame.streamId() + "."));
            if (stream.reassembly != null) return Stages.failed(new ProtocolViolationException("New DATA arrived before fragmented item completion."));
            if (stream.inboundCredit <= 0) return Stages.failed(new ProtocolViolationException("Peer exceeded granted demand on stream " + frame.streamId() + "."));
            stream.inboundCredit -= 1;
        }
        boolean fragmented = (frame.flags() & FrameCodec.DATA_FRAGMENTED_FLAG) != 0;
        if (!fragmented) {
            if (frame.payload().length > runtime.limits().maxInboundItemBytes()) return Stages.failed(new ProtocolViolationException("Peer DATA item exceeds maxInboundItemBytes."));
            if (!stream.incoming.emit(new StreamMessage(frame.payload(), frame.attributes()))) return failInboundStream(stream, new PrpException("Inbound stream item queue is exhausted.", "RESOURCE_EXHAUSTED"));
            return Stages.completed();
        }
        int total = Math.toIntExact(frame.fragmentLength());
        if (total > runtime.limits().maxInboundItemBytes()) return Stages.failed(new ProtocolViolationException("Peer fragmented DATA item exceeds maxInboundItemBytes."));
        synchronized (this) {
            if ((long) inFlightReassemblyBytes + total > runtime.limits().maxInFlightReassemblyBytes()) return failInboundStream(stream, new PrpException("Fragment reassembly byte budget is exhausted.", "RESOURCE_EXHAUSTED"));
            inFlightReassemblyBytes += total;
        }
        try {
            Reassembly reassembly = new Reassembly(new byte[total], snapshot(frame.attributes()), frame.payload().length);
            System.arraycopy(frame.payload(), 0, reassembly.data, 0, frame.payload().length);
            synchronized (stream) { stream.reassembly = reassembly; }
            return Stages.completed();
        } catch (Throwable error) {
            synchronized (this) { inFlightReassemblyBytes = Math.max(0, inFlightReassemblyBytes - total); }
            return failInboundStream(stream, new PrpException("Unable to reserve memory for fragmented DATA item.", "RESOURCE_EXHAUSTED", error));
        }
    }

    private CompletionStage<Void> handleFragment(StreamState stream, ProtocolFrame frame) {
        StreamMessage message = null;
        synchronized (stream) {
            if (stream.inboundClosed || stream.cancelled) return Stages.failed(new ProtocolViolationException("FRAGMENT received for closed stream."));
            Reassembly reassembly = stream.reassembly;
            if (reassembly == null) return Stages.failed(new ProtocolViolationException("FRAGMENT arrived without active fragmented DATA."));
            if (reassembly.offset + frame.payload().length > reassembly.data.length) return Stages.failed(new ProtocolViolationException("FRAGMENT exceeds declared logical DATA length."));
            System.arraycopy(frame.payload(), 0, reassembly.data, reassembly.offset, frame.payload().length);
            reassembly.offset += frame.payload().length;
            if (reassembly.offset == reassembly.data.length) {
                message = new StreamMessage(reassembly.data, reassembly.attributes);
                clearReassembly(stream);
            }
        }
        if (message != null && !stream.incoming.emit(message)) return failInboundStream(stream, new PrpException("Inbound stream item queue is exhausted.", "RESOURCE_EXHAUSTED"));
        return Stages.completed();
    }

    private CompletionStage<Void> handleDemand(StreamState stream, long credit) {
        if (credit <= 0) return Stages.failed(new ProtocolViolationException("DEMAND frame has no positive credit."));
        List<CompletableFuture<Void>> waiters;
        synchronized (stream) {
            stream.outboundCredit = Math.min(MAX_CREDIT, stream.outboundCredit + credit);
            waiters = new ArrayList<>(stream.creditWaiters);
            stream.creditWaiters.clear();
        }
        waiters.forEach(waiter -> waiter.complete(null));
        return Stages.completed();
    }

    private CompletionStage<Void> handleComplete(StreamState stream) {
        synchronized (stream) {
            if (stream.reassembly != null) return Stages.failed(new ProtocolViolationException("COMPLETE interrupted a fragmented DATA item."));
            stream.inboundClosed = true;
        }
        stream.incoming.complete();
        synchronized (this) { releaseIfDone(stream); }
        return Stages.completed();
    }

    private CompletionStage<Void> failInboundStream(StreamState stream, PrpException error) {
        cancelStream(stream, error, true);
        return writeTerminal(FrameKind.ERROR, stream.id, List.of(ProtocolAttribute.text(ERROR_CODE, error.code())), boundedUtf8(error.getMessage()));
    }

    private CompletionStage<Void> requestInbound(StreamState stream, long requested) {
        if (requested <= 0) return Stages.failed(new IllegalArgumentException("Demand must be positive."));
        synchronized (stream) {
            if (stream.cancelled || stream.inboundClosed || state == SessionState.CLOSED || state == SessionState.DETACHED) return Stages.failed(new StreamClosedException());
        }
        long toGrant;
        synchronized (stream) {
            toGrant = Math.min(requested, MAX_CREDIT - stream.inboundCredit);
            if (toGrant <= 0) return Stages.completed();
            stream.inboundCredit += toGrant;
        }
        long grant = toGrant;
        return writeFrame(FrameKind.DEMAND, stream.id, 0, grant, List.of(), new byte[0], false).exceptionally(error -> {
            synchronized (stream) { stream.inboundCredit = Math.max(0, stream.inboundCredit - grant); }
            throw new java.util.concurrent.CompletionException(Stages.unwrap(error));
        });
    }

    private CompletionStage<Void> sendData(StreamState stream, byte[] data, List<ProtocolAttribute> attributes) {
        byte[] payload = data == null ? new byte[0] : data.clone();
        List<ProtocolAttribute> snapshot = snapshot(attributes);
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (stream) {
            stream.writeTail = stream.writeTail.handle((ignored, error) -> null).thenCompose(ignored -> waitForOutboundCredit(stream).thenCompose(nothing -> {
                synchronized (stream) {
                    ensureOutboundOpen(stream);
                    stream.outboundCredit -= 1;
                }
                return writeDataItem(stream, payload, snapshot).exceptionally(writeError -> {
                    synchronized (stream) {
                        if (!stream.cancelled && !stream.outboundClosed) stream.outboundCredit = Math.min(MAX_CREDIT, stream.outboundCredit + 1);
                    }
                    throw new java.util.concurrent.CompletionException(Stages.unwrap(writeError));
                });
            })).whenComplete((ignored, error) -> {
                if (error != null) result.completeExceptionally(Stages.unwrap(error));
                else result.complete(null);
            }).toCompletableFuture();
        }
        return result;
    }

    private CompletionStage<Void> writeDataItem(StreamState stream, byte[] data, List<ProtocolAttribute> attributes) {
        if (data.length > remoteMaxInboundItemBytes) return Stages.failed(new PrpException("Logical DATA item exceeds peer maxInboundItemBytes (" + remoteMaxInboundItemBytes + ").", "ITEM_TOO_LARGE"));
        int attributeBytes;
        try { attributeBytes = FrameCodec.measureAttributeBytes(attributes, outboundLimits); }
        catch (Throwable error) { return Stages.failed(error); }
        int firstCapacity = outboundLimits.maxFrameBytes() - FrameCodec.FRAME_HEADER_BYTES - attributeBytes;
        if (firstCapacity < 0) return Stages.failed(new IllegalArgumentException("DATA attributes do not fit the negotiated frame limit."));
        if (data.length <= firstCapacity) {
            ProtocolFrame validation = new ProtocolFrame(FrameKind.DATA, stream.id, BigInteger.ONE, 0, 0, attributes, data);
            try { FrameCodec.encode(validation, outboundLimits); }
            catch (Throwable error) { return Stages.failed(error); }
            return writeFrame(FrameKind.DATA, stream.id, 0, 0, attributes, data, false);
        }
        if (!negotiated.has(Capabilities.FRAGMENTATION)) return Stages.failed(new CapabilityMismatchException("Required capability was not negotiated: " + Capabilities.FRAGMENTATION));
        int fragmentCapacity = outboundLimits.maxFrameBytes() - FrameCodec.FRAME_HEADER_BYTES;
        if (fragmentCapacity <= 0) return Stages.failed(new IllegalArgumentException("Negotiated frame limit leaves no payload space for fragments."));
        int firstBytes = Math.max(0, firstCapacity);
        int remaining = data.length - firstBytes;
        int fragmentCount = (remaining + fragmentCapacity - 1) / fragmentCapacity;
        int frameCount = 1 + fragmentCount;
        return enqueueWrite(() -> {
            CompletableFuture<Void> tail = CompletableFuture.completedFuture(null);
            for (int index = 0; index < frameCount; index++) {
                int current = index;
                tail = tail.thenCompose(ignored -> {
                    synchronized (stream) { ensureOutboundOpen(stream); }
                    if (current == 0) return writeFrameNow(FrameKind.DATA, stream.id, FrameCodec.DATA_FRAGMENTED_FLAG, data.length, attributes, Arrays.copyOfRange(data, 0, firstBytes), false);
                    int offset = firstBytes + (current - 1) * fragmentCapacity;
                    int end = Math.min(data.length, offset + fragmentCapacity);
                    return writeFrameNow(FrameKind.FRAGMENT, stream.id, 0, 0, List.of(), Arrays.copyOfRange(data, offset, end), false);
                }).toCompletableFuture();
            }
            return tail;
        });
    }

    private CompletionStage<Void> waitForOutboundCredit(StreamState stream) {
        synchronized (stream) {
            ensureOutboundOpen(stream);
            if (stream.outboundCredit > 0) return Stages.completed();
            CompletableFuture<Void> waiter = new CompletableFuture<>();
            stream.creditWaiters.add(waiter);
            return waiter;
        }
    }

    private CompletionStage<Void> completeOutbound(StreamState stream) {
        synchronized (stream) {
            if (stream.outboundClosed || stream.cancelled) return Stages.completed();
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        synchronized (stream) {
            stream.writeTail = stream.writeTail.handle((ignored, error) -> null).thenCompose(ignored -> {
                synchronized (stream) { if (stream.outboundClosed || stream.cancelled) return Stages.completed(); }
                return writeFrame(FrameKind.COMPLETE, stream.id, 0, 0, List.of(), new byte[0], false).thenRun(() -> {
                    synchronized (stream) { stream.outboundClosed = true; }
                    synchronized (ReactiveSessionImpl.this) { releaseIfDone(stream); }
                });
            }).whenComplete((ignored, error) -> {
                if (error != null) result.completeExceptionally(Stages.unwrap(error)); else result.complete(null);
            }).toCompletableFuture();
        }
        return result;
    }

    private CompletionStage<Void> cancelOutbound(StreamState stream, String reason) {
        synchronized (stream) { if (stream.cancelled) return Stages.completed(); }
        return writeTerminal(FrameKind.CANCEL, stream.id, List.of(), reason == null ? new byte[0] : boundedUtf8(reason)).handle((ignored, error) -> {
            cancelStream(stream, new PrpException(reason == null ? "Local cancelled the stream." : reason, "LOCAL_CANCELLED"), true);
            if (error != null) throw new java.util.concurrent.CompletionException(Stages.unwrap(error));
            return null;
        });
    }

    private CompletionStage<Void> failOutbound(StreamState stream, Throwable error) {
        PrpException normalized = error instanceof PrpException prp ? prp : new PrpException(error == null || error.getMessage() == null ? "Application error." : error.getMessage(), "APPLICATION_ERROR", error);
        return writeTerminal(FrameKind.ERROR, stream.id, List.of(ProtocolAttribute.text(ERROR_CODE, normalized.code())), boundedUtf8(normalized.getMessage())).handle((ignored, writeError) -> {
            cancelStream(stream, normalized, true);
            if (writeError != null) throw new java.util.concurrent.CompletionException(Stages.unwrap(writeError));
            return null;
        });
    }

    private CompletionStage<Void> writeFrame(FrameKind kind, BigInteger streamId, int flags, long headerValue, List<ProtocolAttribute> attributes, byte[] payload, boolean bootstrap) {
        return enqueueWrite(() -> writeFrameNow(kind, streamId, flags, headerValue, attributes, payload, bootstrap));
    }

    private CompletionStage<Void> writeFrameNow(FrameKind kind, BigInteger streamId, int flags, long headerValue, List<ProtocolAttribute> attributes, byte[] payload, boolean bootstrap) {
        TransportLane currentLane = lane;
        if (currentLane == null) return Stages.failed(new ConnectionLostException("No physical lane is attached to the session."));
        BigInteger sequence;
        byte[] encoded;
        synchronized (this) {
            if (outgoingSequence.compareTo(MAX_U64) > 0) {
                PrpException error = new PrpException("Peer sequence space is exhausted.", "SEQUENCE_EXHAUSTED");
                detach(error);
                return Stages.failed(error);
            }
            sequence = outgoingSequence;
            ProtocolFrame frame = new ProtocolFrame(kind, streamId, sequence, flags, headerValue, attributes, payload);
            try { encoded = FrameCodec.encode(frame, bootstrap ? ProtocolLimits.BOOTSTRAP : outboundLimits); }
            catch (Throwable error) { return Stages.failed(error); }
        }
        CompletionStage<Void> write;
        try { write = currentLane.write(encoded); }
        catch (Throwable error) { detach(error); return Stages.failed(error); }
        return write.handle((ignored, error) -> {
            if (error != null) {
                Throwable cause = Stages.unwrap(error);
                ConnectionLostException lost = cause instanceof ConnectionLostException connectionLost ? connectionLost : new ConnectionLostException(cause.getMessage() == null ? "Transport write failed." : cause.getMessage(), cause);
                detach(lost);
                throw new java.util.concurrent.CompletionException(lost);
            }
            synchronized (ReactiveSessionImpl.this) { outgoingSequence = sequence.add(BigInteger.ONE); }
            return null;
        });
    }

    private CompletionStage<Void> writeTerminal(FrameKind kind, BigInteger streamId, List<ProtocolAttribute> attributes, byte[] payload) {
        return writeFrame(kind, streamId, 0, 0, attributes, payload, false).handle((ignored, error) -> {
            if (error == null) return Stages.completed();
            return writeFrame(kind, streamId, 0, 0, List.of(), new byte[0], false);
        }).thenCompose(stage -> stage);
    }

    private CompletionStage<Void> enqueueWrite(Supplier<CompletionStage<Void>> operation) {
        CompletableFuture<Void> previous;
        CompletableFuture<Void> gate = new CompletableFuture<>();
        synchronized (this) {
            previous = writeTail;
            writeTail = gate;
        }
        CompletableFuture<Void> result = new CompletableFuture<>();
        previous.handle((ignored, priorError) -> null).thenCompose(ignored -> {
            try { return operation.get(); }
            catch (Throwable error) { return Stages.failed(error); }
        }).whenComplete((ignored, error) -> {
            if (error != null) result.completeExceptionally(Stages.unwrap(error));
            else result.complete(null);
            gate.complete(null);
        });
        return result;
    }

    private CompletionStage<Void> writeSessionError(Throwable error) {
        String code = error instanceof PrpException prp ? prp.code() : "NEGOTIATION_ERROR";
        String message = error == null || error.getMessage() == null ? "Negotiation failed." : error.getMessage();
        return writeTerminal(FrameKind.ERROR, BigInteger.ZERO, List.of(ProtocolAttribute.text(ERROR_CODE, code)), boundedUtf8(message));
    }

    private void startLiveness() {
        stopLiveness();
        long interval = runtime.liveness().interval().toMillis();
        livenessTask = TIMER.scheduleWithFixedDelay(() -> {
            if (state != SessionState.READY) return;
            long silentFor = Duration.ofNanos(System.nanoTime() - lastInboundNanos).toMillis();
            if (silentFor >= runtime.liveness().timeout().toMillis()) {
                detach(new LivenessTimeoutException("No inbound PRP frame was received for " + silentFor + "ms."));
                return;
            }
            if (silentFor >= interval) {
                BigInteger probe;
                synchronized (this) {
                    probe = nextProbeId;
                    nextProbeId = nextProbeId.compareTo(MAX_U64) >= 0 ? BigInteger.ONE : nextProbeId.add(BigInteger.ONE);
                }
                writeFrame(FrameKind.PING, BigInteger.ZERO, 0, 0, List.of(), LivenessCodec.encodeProbe(probe), false);
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    private void stopLiveness() {
        java.util.concurrent.ScheduledFuture<?> task = livenessTask;
        livenessTask = null;
        if (task != null) task.cancel(false);
    }

    private CompletionStage<Void> finishClosed(String reason) {
        List<AutoCloseable> disposers;
        TransportLane currentLane;
        TransportLane currentDatagramLane;
        TransportConnection currentConnection;
        synchronized (this) {
            if (state == SessionState.CLOSED) return Stages.completed();
            stopLiveness();
            StreamClosedException failure = new StreamClosedException(reason);
            for (StreamState stream : new ArrayList<>(streams.values())) cancelStream(stream, failure, false);
            streams.clear();
            incomingStreams.complete();
            incomingSignals.complete();
            incomingDatagrams.complete();
            pendingSignalBytes = 0;
            currentLane = lane;
            currentDatagramLane = datagramLane;
            currentConnection = connection;
            lane = null;
            datagramLane = null;
            remoteMaxInboundDatagramBytes = 0;
            connection = null;
            transition(SessionState.CLOSED);
            disposers = new ArrayList<>(extensionDisposers);
            extensionDisposers.clear();
        }
        java.util.Collections.reverse(disposers);
        for (AutoCloseable disposer : disposers) try { disposer.close(); } catch (Exception ignored) {}
        CompletionStage<Void> laneClose = currentLane == null ? Stages.completed() : currentLane.close(reason).exceptionally(error -> null);
        CompletionStage<Void> datagramClose = currentDatagramLane == null ? Stages.completed() : currentDatagramLane.close(reason).exceptionally(error -> null);
        return laneClose.thenCompose(ignored -> datagramClose).thenCompose(ignored -> currentConnection == null ? Stages.completed() : currentConnection.close(null, reason).exceptionally(error -> null));
    }

    private void detach(Throwable error) {
        List<AutoCloseable> disposers;
        TransportLane currentLane;
        TransportLane currentDatagramLane;
        TransportConnection currentConnection;
        synchronized (this) {
            if (state == SessionState.CLOSED || state == SessionState.DETACHED) return;
            stopLiveness();
            Throwable failure = error instanceof PrpException ? error : error == null ? new ConnectionLostException() : new ConnectionLostException(error.getMessage() == null ? "Connection lost." : error.getMessage(), error);
            ready.completeExceptionally(failure);
            for (StreamState stream : new ArrayList<>(streams.values())) cancelStream(stream, failure, false);
            streams.clear();
            incomingStreams.fail(failure);
            incomingSignals.fail(failure);
            incomingDatagrams.fail(failure);
            currentLane = lane;
            currentDatagramLane = datagramLane;
            currentConnection = connection;
            lane = null;
            datagramLane = null;
            remoteMaxInboundDatagramBytes = 0;
            connection = null;
            transition(SessionState.DETACHED);
            disposers = new ArrayList<>(extensionDisposers);
            extensionDisposers.clear();
        }
        java.util.Collections.reverse(disposers);
        for (AutoCloseable disposer : disposers) try { disposer.close(); } catch (Exception ignored) {}
        if (currentLane != null) currentLane.close("detached");
        if (currentDatagramLane != null) currentDatagramLane.close("detached");
        if (currentConnection != null) currentConnection.close(null, "detached");
    }

    private void cancelStream(StreamState stream, Throwable error, boolean allowLateData) {
        List<CompletableFuture<Void>> waiters;
        synchronized (stream) {
            if (stream.cancelled) return;
            clearReassembly(stream);
            stream.cancelled = true;
            stream.inboundClosed = true;
            stream.outboundClosed = true;
            waiters = new ArrayList<>(stream.creditWaiters);
            stream.creditWaiters.clear();
        }
        stream.incoming.fail(error);
        waiters.forEach(waiter -> waiter.completeExceptionally(error));
        synchronized (this) {
            releaseState(stream, false);
            retire(stream.id, allowLateData);
        }
    }

    private void releaseIfDone(StreamState stream) {
        synchronized (stream) {
            if (!((stream.inboundClosed && stream.outboundClosed) || stream.cancelled)) return;
        }
        releaseState(stream, false);
        retire(stream.id, false);
    }

    private void releaseState(StreamState stream, boolean abort) {
        if (!streams.remove(stream.id, stream)) return;
        if (stream.initiatedLocally) localActiveStreams = Math.max(0, localActiveStreams - 1);
        else remoteActiveStreams = Math.max(0, remoteActiveStreams - 1);
        retainedStreamAttributeBytes = Math.max(0, retainedStreamAttributeBytes - stream.retainedAttributeBytes);
        if (abort) {
            synchronized (stream) {
                stream.cancelled = true;
                stream.inboundClosed = true;
                stream.outboundClosed = true;
            }
        }
    }

    private void clearReassembly(StreamState stream) {
        synchronized (stream) {
            if (stream.reassembly == null) return;
            synchronized (this) { inFlightReassemblyBytes = Math.max(0, inFlightReassemblyBytes - stream.reassembly.data.length); }
            stream.reassembly = null;
        }
    }

    private void retire(BigInteger streamId, boolean allowLateData) {
        if (retiredStreams.containsKey(streamId)) return;
        retiredStreams.put(streamId, allowLateData);
        while (retiredStreams.size() > runtime.limits().maxRetiredStreams()) retiredStreams.remove(retiredStreams.keySet().iterator().next());
    }

    private boolean isPastStreamId(BigInteger streamId) {
        if (streamId.signum() <= 0 || origin == null) return false;
        BigInteger localParity = origin == SessionOrigin.INITIATOR ? BigInteger.ONE : BigInteger.ZERO;
        if (streamId.mod(TWO).equals(localParity)) return streamId.compareTo(nextStreamId) < 0;
        return streamId.compareTo(highestRemoteStreamId) <= 0;
    }

    private StreamState createStreamState(BigInteger id, List<ProtocolAttribute> attributes, boolean initiatedLocally) {
        List<ProtocolAttribute> snapshot = snapshot(attributes);
        StreamState state = new StreamState(id, snapshot, initiatedLocally, attributeMemoryBytes(snapshot));
        state.incoming = new BoundedPublisher<>(Math.max(64, Math.min(65536, runtime.limits().maxPendingIncomingStreams())),
            requested -> requestInbound(state, requested),
            () -> cancelOutbound(state, "Reactive consumer cancelled."));
        return state;
    }

    private void ensureOutboundOpen(StreamState stream) {
        if (stream.cancelled || stream.outboundClosed || state == SessionState.CLOSED || state == SessionState.DETACHED) throw new StreamClosedException();
    }

    private synchronized void assertReady() {
        if (state != SessionState.READY) throw new PrpException("Session is not ready; current state is " + state + ".", "SESSION_NOT_READY");
    }

    private void transition(SessionState next) {
        state = next;
        for (Consumer<SessionState> listener : stateListeners) try { listener.accept(next); } catch (Throwable ignored) {}
    }

    private static List<ProtocolAttribute> snapshot(List<ProtocolAttribute> attributes) {
        if (attributes == null || attributes.isEmpty()) return List.of();
        List<ProtocolAttribute> output = new ArrayList<>(attributes.size());
        for (ProtocolAttribute attribute : attributes) output.add(new ProtocolAttribute(attribute.id(), attribute.value(), attribute.required()));
        return List.copyOf(output);
    }

    private static int attributeMemoryBytes(List<ProtocolAttribute> attributes) {
        int total = 0;
        for (ProtocolAttribute attribute : attributes) total += PrpText.utf8(attribute.id()).length + attribute.value().length + 7;
        return total;
    }

    private static byte[] boundedUtf8(String message) {
        byte[] source = PrpText.utf8(message == null ? "" : message);
        if (source.length <= MAX_ERROR_REASON_BYTES) return source;
        int end = MAX_ERROR_REASON_BYTES;
        while (end > 0 && (source[end] & 0xc0) == 0x80) end -= 1;
        return Arrays.copyOf(source, end);
    }

    private static PrpException remoteError(ProtocolFrame frame, String fallbackMessage, String fallbackCode) {
        String code = fallbackCode;
        for (ProtocolAttribute attribute : frame.attributes()) {
            if (ERROR_CODE.equals(attribute.id())) code = PrpText.strictText(attribute.value());
            else if (attribute.required()) return new PrpException("Unknown required ERROR attribute: " + attribute.id(), "PROTOCOL_VIOLATION");
        }
        String message = frame.payload().length == 0 ? fallbackMessage : remoteReason(frame, fallbackMessage);
        return new PrpException(message, code);
    }

    private static String remoteReason(ProtocolFrame frame, String fallback) {
        if (frame.payload().length == 0) return fallback;
        String value = PrpText.strictText(frame.payload());
        return value.isEmpty() ? fallback : value;
    }

    private final class ReactiveStreamImpl implements ReactiveStream {
        private final StreamState stream;
        private ReactiveStreamImpl(StreamState stream) { this.stream = stream; }
        @Override public BigInteger id() { return stream.id; }
        @Override public List<ProtocolAttribute> attributes() { return stream.attributes; }
        @Override public boolean closed() { synchronized (stream) { return stream.cancelled || (stream.inboundClosed && stream.outboundClosed); } }
        @Override public CompletionStage<Void> request(long count) { return requestInbound(stream, count); }
        @Override public CompletionStage<Void> send(byte[] data, List<ProtocolAttribute> attributes) { return sendData(stream, data, attributes); }
        @Override public CompletionStage<Void> complete() { return completeOutbound(stream); }
        @Override public CompletionStage<Void> cancel(String reason) { return cancelOutbound(stream, reason); }
        @Override public CompletionStage<Void> fail(Throwable error) { return failOutbound(stream, error); }
        @Override public void subscribe(Flow.Subscriber<? super StreamMessage> subscriber) { stream.incoming.subscribe(subscriber); }
    }

    private static final class StreamState {
        private final BigInteger id;
        private final List<ProtocolAttribute> attributes;
        private final boolean initiatedLocally;
        private final int retainedAttributeBytes;
        private BoundedPublisher<StreamMessage> incoming;
        private long inboundCredit;
        private long outboundCredit;
        private boolean outboundClosed;
        private boolean inboundClosed;
        private boolean cancelled;
        private CompletableFuture<Void> writeTail = CompletableFuture.completedFuture(null);
        private final ArrayDeque<CompletableFuture<Void>> creditWaiters = new ArrayDeque<>();
        private Reassembly reassembly;

        private StreamState(BigInteger id, List<ProtocolAttribute> attributes, boolean initiatedLocally, int retainedAttributeBytes) {
            this.id = id;
            this.attributes = attributes;
            this.initiatedLocally = initiatedLocally;
            this.retainedAttributeBytes = retainedAttributeBytes;
        }
    }

    private static final class Reassembly {
        private final byte[] data;
        private final List<ProtocolAttribute> attributes;
        private int offset;
        private Reassembly(byte[] data, List<ProtocolAttribute> attributes, int offset) {
            this.data = data;
            this.attributes = attributes;
            this.offset = offset;
        }
    }
}
