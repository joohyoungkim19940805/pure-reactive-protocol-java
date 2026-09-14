package com.byeolnaerim.prp;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public interface ReactiveStream extends Flow.Publisher<StreamMessage> {
    BigInteger id();
    List<ProtocolAttribute> attributes();
    boolean closed();
    CompletionStage<Void> request(long count);
    CompletionStage<Void> send(byte[] data, List<ProtocolAttribute> attributes);
    default CompletionStage<Void> send(byte[] data) { return send(data, List.of()); }
    default CompletionStage<Void> send() { return send(new byte[0], List.of()); }
    CompletionStage<Void> complete();
    CompletionStage<Void> cancel(String reason);
    default CompletionStage<Void> cancel() { return cancel(null); }
    CompletionStage<Void> fail(Throwable error);
}
