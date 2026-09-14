package com.byeolnaerim.prp.spring;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public interface PrpRequester {
    <I, O> CompletionStage<O> requestResponse(String route, I input, Class<O> responseType);
    <I> CompletionStage<Void> fireAndForget(String route, I input);
    <I, O> CompletionStage<Flow.Publisher<O>> requestStream(String route, I input, Class<O> responseType);
    <I, O> CompletionStage<Flow.Publisher<O>> requestChannel(String route, Flow.Publisher<I> input, Class<O> responseType);
}
