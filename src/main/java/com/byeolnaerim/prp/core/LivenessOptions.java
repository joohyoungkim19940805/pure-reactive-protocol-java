package com.byeolnaerim.prp.core;

import java.time.Duration;

public record LivenessOptions(Duration interval, Duration timeout) {
    public static final LivenessOptions DEFAULT = new LivenessOptions(Duration.ofSeconds(15), Duration.ofSeconds(45));

    public LivenessOptions {
        if (interval == null || timeout == null || interval.isZero() || interval.isNegative() || timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("Liveness interval and timeout must be positive.");
        }
        if (timeout.compareTo(interval) <= 0) throw new IllegalArgumentException("Liveness timeout must be greater than interval.");
        if (interval.toMillis() > 0x7fffffffL || timeout.toMillis() > 0x7fffffffL) throw new IllegalArgumentException("Liveness values must fit unsigned 31-bit milliseconds.");
    }
}
