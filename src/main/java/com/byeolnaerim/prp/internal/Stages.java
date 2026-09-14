package com.byeolnaerim.prp.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

public final class Stages {
    private Stages() {}

    public static CompletableFuture<Void> completed() { return CompletableFuture.completedFuture(null); }

    public static <T> CompletableFuture<T> failed(Throwable error) {
        return CompletableFuture.failedFuture(unwrap(error));
    }

    public static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException) && current.getCause() != null) current = current.getCause();
        return current;
    }

    public static CompletableFuture<Void> toFuture(CompletionStage<Void> stage) {
        return stage.toCompletableFuture();
    }
}
