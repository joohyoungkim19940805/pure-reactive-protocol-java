package com.byeolnaerim.prp.transport;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public interface TransportLane {
    String id();
    default int maxFrameBytes() { return Integer.MAX_VALUE; }
    Flow.Publisher<byte[]> incoming();
    CompletionStage<Void> write(byte[] frame);
    CompletionStage<Void> close(String reason);
}
