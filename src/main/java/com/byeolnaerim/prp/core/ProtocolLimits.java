package com.byeolnaerim.prp.core;

public record ProtocolLimits(
    int maxFrameBytes,
    int maxAttributeBytes,
    int maxAttributes,
    int maxAttributeIdBytes,
    int maxAttributeValueBytes,
    int maxInboundItemBytes,
    int maxInFlightReassemblyBytes,
    int maxInboundStreams,
    int maxPendingIncomingStreams,
    int maxPendingSignals,
    int maxInFlightSignalBytes,
    int maxRetainedStreamAttributeBytes,
    int maxRetiredStreams
) {
    public static final ProtocolLimits DEFAULT = new ProtocolLimits(
        16 * 1024 * 1024,
        1024 * 1024,
        256,
        1024,
        1024 * 1024,
        64 * 1024 * 1024,
        128 * 1024 * 1024,
        4096,
        1024,
        1024,
        16 * 1024 * 1024,
        64 * 1024 * 1024,
        4096
    );

    public static final ProtocolLimits BOOTSTRAP = new ProtocolLimits(
        64 * 1024,
        60 * 1024,
        256,
        1024,
        16 * 1024,
        DEFAULT.maxInboundItemBytes,
        DEFAULT.maxInFlightReassemblyBytes,
        DEFAULT.maxInboundStreams,
        DEFAULT.maxPendingIncomingStreams,
        DEFAULT.maxPendingSignals,
        DEFAULT.maxInFlightSignalBytes,
        DEFAULT.maxRetainedStreamAttributeBytes,
        DEFAULT.maxRetiredStreams
    );

    public ProtocolLimits {
        positive("maxFrameBytes", maxFrameBytes);
        positive("maxAttributeBytes", maxAttributeBytes);
        positive("maxAttributes", maxAttributes);
        positive("maxAttributeIdBytes", maxAttributeIdBytes);
        positive("maxAttributeValueBytes", maxAttributeValueBytes);
        positive("maxInboundItemBytes", maxInboundItemBytes);
        positive("maxInFlightReassemblyBytes", maxInFlightReassemblyBytes);
        positive("maxInboundStreams", maxInboundStreams);
        positive("maxPendingIncomingStreams", maxPendingIncomingStreams);
        positive("maxPendingSignals", maxPendingSignals);
        positive("maxInFlightSignalBytes", maxInFlightSignalBytes);
        positive("maxRetainedStreamAttributeBytes", maxRetainedStreamAttributeBytes);
        positive("maxRetiredStreams", maxRetiredStreams);
        if (maxFrameBytes < FrameCodec.FRAME_HEADER_BYTES) throw new IllegalArgumentException("maxFrameBytes must fit the PRP/1 header.");
        if (maxAttributeBytes >= maxFrameBytes) throw new IllegalArgumentException("maxAttributeBytes must be smaller than maxFrameBytes.");
        if (maxAttributeIdBytes > 0xffff) throw new IllegalArgumentException("maxAttributeIdBytes must fit unsigned 16 bits.");
        if (maxAttributeValueBytes > maxAttributeBytes) throw new IllegalArgumentException("maxAttributeValueBytes must not exceed maxAttributeBytes.");
        if (maxInFlightReassemblyBytes < maxInboundItemBytes) throw new IllegalArgumentException("maxInFlightReassemblyBytes must be at least maxInboundItemBytes.");
        if (maxPendingIncomingStreams > maxInboundStreams) throw new IllegalArgumentException("maxPendingIncomingStreams must not exceed maxInboundStreams.");
    }

    private static void positive(String name, int value) {
        if (value <= 0) throw new IllegalArgumentException(name + " must be positive.");
    }

    public static Builder builder() { return new Builder(DEFAULT); }
    public static Builder builder(ProtocolLimits base) { return new Builder(base); }

    public static final class Builder {
        private int maxFrameBytes;
        private int maxAttributeBytes;
        private int maxAttributes;
        private int maxAttributeIdBytes;
        private int maxAttributeValueBytes;
        private int maxInboundItemBytes;
        private int maxInFlightReassemblyBytes;
        private int maxInboundStreams;
        private int maxPendingIncomingStreams;
        private int maxPendingSignals;
        private int maxInFlightSignalBytes;
        private int maxRetainedStreamAttributeBytes;
        private int maxRetiredStreams;

        private Builder(ProtocolLimits base) {
            this.maxFrameBytes = base.maxFrameBytes;
            this.maxAttributeBytes = base.maxAttributeBytes;
            this.maxAttributes = base.maxAttributes;
            this.maxAttributeIdBytes = base.maxAttributeIdBytes;
            this.maxAttributeValueBytes = base.maxAttributeValueBytes;
            this.maxInboundItemBytes = base.maxInboundItemBytes;
            this.maxInFlightReassemblyBytes = base.maxInFlightReassemblyBytes;
            this.maxInboundStreams = base.maxInboundStreams;
            this.maxPendingIncomingStreams = Math.min(base.maxPendingIncomingStreams, base.maxInboundStreams);
            this.maxPendingSignals = base.maxPendingSignals;
            this.maxInFlightSignalBytes = base.maxInFlightSignalBytes;
            this.maxRetainedStreamAttributeBytes = base.maxRetainedStreamAttributeBytes;
            this.maxRetiredStreams = base.maxRetiredStreams;
        }

        public Builder maxFrameBytes(int value) { maxFrameBytes = value; return this; }
        public Builder maxAttributeBytes(int value) { maxAttributeBytes = value; return this; }
        public Builder maxAttributes(int value) { maxAttributes = value; return this; }
        public Builder maxAttributeIdBytes(int value) { maxAttributeIdBytes = value; return this; }
        public Builder maxAttributeValueBytes(int value) { maxAttributeValueBytes = value; return this; }
        public Builder maxInboundItemBytes(int value) { maxInboundItemBytes = value; return this; }
        public Builder maxInFlightReassemblyBytes(int value) { maxInFlightReassemblyBytes = value; return this; }
        public Builder maxInboundStreams(int value) { maxInboundStreams = value; maxPendingIncomingStreams = Math.min(maxPendingIncomingStreams, value); return this; }
        public Builder maxPendingIncomingStreams(int value) { maxPendingIncomingStreams = value; return this; }
        public Builder maxPendingSignals(int value) { maxPendingSignals = value; return this; }
        public Builder maxInFlightSignalBytes(int value) { maxInFlightSignalBytes = value; return this; }
        public Builder maxRetainedStreamAttributeBytes(int value) { maxRetainedStreamAttributeBytes = value; return this; }
        public Builder maxRetiredStreams(int value) { maxRetiredStreams = value; return this; }

        public ProtocolLimits build() {
            return new ProtocolLimits(maxFrameBytes, maxAttributeBytes, maxAttributes, maxAttributeIdBytes, maxAttributeValueBytes, maxInboundItemBytes, maxInFlightReassemblyBytes, maxInboundStreams, maxPendingIncomingStreams, maxPendingSignals, maxInFlightSignalBytes, maxRetainedStreamAttributeBytes, maxRetiredStreams);
        }
    }

    public ProtocolLimits outboundFor(PeerProtocolLimits peer) {
        return new ProtocolLimits(
            Math.min(maxFrameBytes, peer.maxFrameBytes()),
            Math.min(maxAttributeBytes, peer.maxAttributeBytes()),
            Math.min(maxAttributes, peer.maxAttributes()),
            Math.min(maxAttributeIdBytes, peer.maxAttributeIdBytes()),
            Math.min(maxAttributeValueBytes, peer.maxAttributeValueBytes()),
            maxInboundItemBytes,
            maxInFlightReassemblyBytes,
            maxInboundStreams,
            maxPendingIncomingStreams,
            maxPendingSignals,
            maxInFlightSignalBytes,
            maxRetainedStreamAttributeBytes,
            maxRetiredStreams
        );
    }
}
