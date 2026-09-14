package com.byeolnaerim.prp.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Function;
import java.util.function.Supplier;

public final class FlowSupport {
    private FlowSupport() {}

    public static <I, O> Flow.Publisher<O> map(Flow.Publisher<I> source, Function<I, O> mapper) {
        return subscriber -> source.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;
            private boolean done;
            @Override public void onSubscribe(Flow.Subscription subscription) {
                this.subscription = subscription;
                subscriber.onSubscribe(new Flow.Subscription() {
                    @Override public void request(long n) { subscription.request(n); }
                    @Override public void cancel() { subscription.cancel(); }
                });
            }
            @Override public void onNext(I item) {
                if (done) return;
                try { subscriber.onNext(mapper.apply(item)); }
                catch (Throwable error) {
                    done = true;
                    subscription.cancel();
                    subscriber.onError(error);
                }
            }
            @Override public void onError(Throwable throwable) { if (!done) { done = true; subscriber.onError(throwable); } }
            @Override public void onComplete() { if (!done) { done = true; subscriber.onComplete(); } }
        });
    }

    public static <T> CompletionStage<T> exactlyOne(Flow.Publisher<T> source) {
        CompletableFuture<T> result = new CompletableFuture<>();
        source.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;
            private T value;
            private int count;
            @Override public void onSubscribe(Flow.Subscription subscription) { this.subscription = subscription; subscription.request(2); }
            @Override public void onNext(T item) {
                count += 1;
                if (count == 1) value = item;
                else {
                    subscription.cancel();
                    result.completeExceptionally(new IllegalStateException("Expected exactly one item, received more than one."));
                }
            }
            @Override public void onError(Throwable throwable) { result.completeExceptionally(throwable); }
            @Override public void onComplete() {
                if (result.isDone()) return;
                if (count != 1) result.completeExceptionally(new IllegalStateException("Expected exactly one item, received " + count + "."));
                else result.complete(value);
            }
        });
        return result;
    }

    public static <T> CompletionStage<Void> pump(Flow.Publisher<T> source, Function<T, CompletionStage<Void>> sink, Supplier<CompletionStage<Void>> onComplete) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        source.subscribe(new Flow.Subscriber<>() {
            private Flow.Subscription subscription;
            private volatile boolean sourceCompleted;
            private volatile boolean writing;
            @Override public void onSubscribe(Flow.Subscription subscription) { this.subscription = subscription; subscription.request(1); }
            @Override public void onNext(T item) {
                writing = true;
                CompletionStage<Void> stage;
                try { stage = sink.apply(item); }
                catch (Throwable error) { subscription.cancel(); result.completeExceptionally(error); return; }
                stage.whenComplete((ignored, error) -> {
                    writing = false;
                    if (error != null) {
                        subscription.cancel();
                        result.completeExceptionally(Stages.unwrap(error));
                    } else if (sourceCompleted) {
                        complete();
                    } else {
                        subscription.request(1);
                    }
                });
            }
            @Override public void onError(Throwable throwable) { result.completeExceptionally(throwable); }
            @Override public void onComplete() {
                sourceCompleted = true;
                if (!writing) complete();
            }
            private void complete() {
                if (result.isDone()) return;
                CompletionStage<Void> stage;
                try { stage = onComplete.get(); }
                catch (Throwable error) { result.completeExceptionally(error); return; }
                stage.whenComplete((ignored, error) -> {
                    if (error != null) result.completeExceptionally(Stages.unwrap(error));
                    else result.complete(null);
                });
            }
        });
        return result;
    }
}
