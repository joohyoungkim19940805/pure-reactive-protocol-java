package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.SessionOrigin;
import com.byeolnaerim.prp.SignalAcceptor;
import com.byeolnaerim.prp.StreamAcceptor;
import com.byeolnaerim.prp.internal.ReactiveSessionImpl;
import com.byeolnaerim.prp.transport.ReactiveTransport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public final class PrpRuntime {
    private final ProtocolLimits limits;
    private final LivenessOptions liveness;
    private final Duration handshakeTimeout;
    private final DatagramOptions datagrams;
    private final List<ProtocolExtension> extensions;
    private final List<CapabilityDescriptor> capabilities;
    private final CapabilityPolicy policy;
    private final List<StreamAcceptor> streamAcceptors;
    private final List<SignalAcceptor> signalAcceptors;

    private PrpRuntime(Builder builder) {
        this.limits = builder.limits;
        this.liveness = builder.liveness;
        this.handshakeTimeout = builder.handshakeTimeout;
        this.datagrams = builder.datagrams;
        this.extensions = List.copyOf(builder.extensions);
        this.streamAcceptors = List.copyOf(builder.streamAcceptors);
        this.signalAcceptors = List.copyOf(builder.signalAcceptors);
        List<CapabilityDescriptor> installed = new ArrayList<>(Capabilities.base(limits, liveness));
        if (datagrams != null) installed.add(new CapabilityDescriptor(DatagramCodec.CAPABILITY_ID, 1, 1, DatagramCodec.encodeCapability(datagrams.maxInboundBytes())));
        Set<String> ids = new LinkedHashSet<>();
        for (CapabilityDescriptor capability : installed) ids.add(capability.id());
        for (ProtocolExtension extension : extensions) {
            CapabilityDescriptor capability = extension.capability();
            if (!ids.add(capability.id())) throw new IllegalArgumentException("Duplicate protocol capability: " + capability.id());
            installed.add(capability);
        }
        for (ProtocolExtension extension : extensions) {
            for (String required : extension.requiredCapabilities()) {
                if (!ids.contains(required)) throw new IllegalArgumentException("Protocol extension " + extension.capability().id() + " requires unavailable capability: " + required);
            }
        }
        Set<String> required = new LinkedHashSet<>();
        for (CapabilityDescriptor capability : Capabilities.base(limits, liveness)) required.add(capability.id());
        required.addAll(builder.requiredCapabilities);
        for (String id : required) if (!ids.contains(id)) throw new IllegalArgumentException("Required capability is not installed in this runtime: " + id);
        this.capabilities = List.copyOf(installed);
        this.policy = new CapabilityPolicy(required);
    }

    public static Builder builder() { return new Builder(); }
    public static PrpRuntime defaults() { return builder().build(); }

    public ProtocolLimits limits() { return limits; }
    public LivenessOptions liveness() { return liveness; }
    public Duration handshakeTimeout() { return handshakeTimeout; }
    public DatagramOptions datagrams() { return datagrams; }
    public List<ProtocolExtension> extensions() { return extensions; }
    public List<CapabilityDescriptor> capabilities() { return capabilities; }
    public CapabilityPolicy policy() { return policy; }
    public List<StreamAcceptor> streamAcceptors() { return streamAcceptors; }
    public List<SignalAcceptor> signalAcceptors() { return signalAcceptors; }

    public CompletionStage<ReactiveSession> connect(ReactiveTransport transport) {
        return new ReactiveSessionImpl(this).attach(transport, SessionOrigin.INITIATOR).thenApply(session -> session);
    }

    public CompletionStage<ReactiveSession> accept(ReactiveTransport transport) {
        return new ReactiveSessionImpl(this).attach(transport, SessionOrigin.ACCEPTOR).thenApply(session -> session);
    }

    public static final class Builder {
        private ProtocolLimits limits = ProtocolLimits.DEFAULT;
        private LivenessOptions liveness = LivenessOptions.DEFAULT;
        private Duration handshakeTimeout = Duration.ofSeconds(30);
        private DatagramOptions datagrams = DatagramOptions.DEFAULT;
        private final List<ProtocolExtension> extensions = new ArrayList<>();
        private final Set<String> requiredCapabilities = new LinkedHashSet<>();
        private final List<StreamAcceptor> streamAcceptors = new ArrayList<>();
        private final List<SignalAcceptor> signalAcceptors = new ArrayList<>();

        public Builder limits(ProtocolLimits limits) { this.limits = java.util.Objects.requireNonNull(limits); return this; }
        public Builder liveness(LivenessOptions liveness) { this.liveness = java.util.Objects.requireNonNull(liveness); return this; }
        public Builder handshakeTimeout(Duration timeout) {
            if (timeout == null || timeout.isZero() || timeout.isNegative()) throw new IllegalArgumentException("handshakeTimeout must be positive.");
            this.handshakeTimeout = timeout;
            return this;
        }
        public Builder datagrams(DatagramOptions datagrams) { this.datagrams = java.util.Objects.requireNonNull(datagrams); return this; }
        public Builder disableDatagrams() { this.datagrams = null; return this; }
        public Builder extension(ProtocolExtension extension) { this.extensions.add(java.util.Objects.requireNonNull(extension)); return this; }
        public Builder require(String capabilityId) { this.requiredCapabilities.add(capabilityId); return this; }
        public Builder streamAcceptor(StreamAcceptor acceptor) { this.streamAcceptors.add(java.util.Objects.requireNonNull(acceptor)); return this; }
        public Builder signalAcceptor(SignalAcceptor acceptor) { this.signalAcceptors.add(java.util.Objects.requireNonNull(acceptor)); return this; }
        public PrpRuntime build() { return new PrpRuntime(this); }
    }
}
