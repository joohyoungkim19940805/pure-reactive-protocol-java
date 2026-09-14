package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.ReactiveSession;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public interface ProtocolExtension {
    CapabilityDescriptor capability();
    default Set<String> requiredCapabilities() { return Set.of(); }
    CompletionStage<AutoCloseable> attach(ReactiveSession session);
}
