package com.byeolnaerim.prp.spring;

import com.byeolnaerim.prp.profile.datagram.RoutedDatagram;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import org.reactivestreams.Publisher;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class PrpRouteInvoker {
    private final Executor blockingExecutor;

    public PrpRouteInvoker(Executor blockingExecutor) {
        this.blockingExecutor = java.util.Objects.requireNonNull(blockingExecutor);
    }

    public Mono<Object> requestResponse(PrpRouteDescriptor descriptor, Object input, PrpContext context) {
        return invokeMono(descriptor, input, context)
            .switchIfEmpty(Mono.error(new IllegalStateException("PRP request-response route returned no value: " + descriptor.route())));
    }

    public Mono<Void> fireAndForget(PrpRouteDescriptor descriptor, Object input, PrpContext context) {
        return invokeMono(descriptor, input, context).then();
    }

    public Flux<Object> requestStream(PrpRouteDescriptor descriptor, Object input, PrpContext context) {
        return invokeFlux(descriptor, input, context);
    }

    public Flux<Object> requestChannel(PrpRouteDescriptor descriptor, Flux<Object> input, PrpContext context) {
        return invokeFlux(descriptor, adaptChannelInput(descriptor, input), context);
    }

    public Mono<Void> datagram(PrpRouteDescriptor descriptor, RoutedDatagram datagram, PrpContext context) {
        return invokeMono(descriptor, adaptDatagramInput(descriptor, datagram), context).then();
    }

    private Mono<Object> invokeMono(PrpRouteDescriptor descriptor, Object payload, PrpContext context) {
        if (isAsyncReturn(descriptor)) {
            return Mono.defer(() -> toMono(invoke(descriptor, payload, context)));
        }
        return Mono.fromCompletionStage(
            CompletableFuture.supplyAsync(() -> invoke(descriptor, payload, context), blockingExecutor)
        );
    }

    private Flux<Object> invokeFlux(PrpRouteDescriptor descriptor, Object payload, PrpContext context) {
        if (isAsyncReturn(descriptor)) {
            return Flux.defer(() -> toFlux(invoke(descriptor, payload, context)));
        }
        return Mono.fromCompletionStage(
            CompletableFuture.supplyAsync(() -> invoke(descriptor, payload, context), blockingExecutor)
        ).flatMapMany(PrpRouteInvoker::toFlux);
    }

    private static boolean isAsyncReturn(PrpRouteDescriptor descriptor) {
        Class<?> type = descriptor.method().getReturnType();
        return CompletionStage.class.isAssignableFrom(type)
            || Flow.Publisher.class.isAssignableFrom(type)
            || Publisher.class.isAssignableFrom(type);
    }

    private static Object invoke(PrpRouteDescriptor descriptor, Object payload, PrpContext context) {
        Object[] arguments = new Object[descriptor.method().getParameterCount()];
        arguments[descriptor.payloadParameterIndex()] = payload;
        if (descriptor.contextParameterIndex() >= 0) arguments[descriptor.contextParameterIndex()] = context;
        try {
            return descriptor.method().invoke(descriptor.bean(), arguments);
        } catch (InvocationTargetException error) {
            Throwable cause = error.getCause() == null ? error : error.getCause();
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error fatal) throw fatal;
            throw new IllegalStateException("PRP route invocation failed: " + descriptor.route(), cause);
        } catch (ReflectiveOperationException error) {
            throw new IllegalStateException("PRP route invocation failed: " + descriptor.route(), error);
        }
    }

    private static Object adaptChannelInput(PrpRouteDescriptor descriptor, Flux<Object> input) {
        Class<?> declared = descriptor.declaredPayloadParameterType();
        if (Flow.Publisher.class.isAssignableFrom(declared)) return JdkFlowAdapter.publisherToFlowPublisher(input);
        return input;
    }

    private static Object adaptDatagramInput(PrpRouteDescriptor descriptor, RoutedDatagram datagram) {
        Class<?> declared = descriptor.declaredPayloadParameterType();
        if (declared == byte[].class) return datagram.data();
        if (ByteBuffer.class.isAssignableFrom(declared)) return ByteBuffer.wrap(datagram.data()).asReadOnlyBuffer();
        if (RoutedDatagram.class.isAssignableFrom(declared)) return datagram;
        throw new IllegalStateException("Unsupported DATAGRAM parameter type: " + declared.getName());
    }

    @SuppressWarnings("unchecked")
    private static Mono<Object> toMono(Object value) {
        if (value == null) return Mono.empty();
        if (value instanceof Mono<?> mono) return (Mono<Object>) mono;
        if (value instanceof CompletionStage<?> stage) return (Mono<Object>) Mono.fromCompletionStage(stage);
        if (value instanceof Flow.Publisher<?> flow) return JdkFlowAdapter.flowPublisherToFlux((Flow.Publisher<Object>) flow).singleOrEmpty();
        if (value instanceof Publisher<?> publisher) return (Mono<Object>) Mono.from((Publisher<Object>) publisher);
        return Mono.just(value);
    }

    @SuppressWarnings("unchecked")
    private static Flux<Object> toFlux(Object value) {
        if (value == null) return Flux.empty();
        if (value instanceof Flow.Publisher<?> flow) return JdkFlowAdapter.flowPublisherToFlux((Flow.Publisher<Object>) flow);
        if (value instanceof Publisher<?> publisher) return Flux.from((Publisher<Object>) publisher);
        if (value instanceof Iterable<?> iterable) return Flux.fromIterable((Iterable<Object>) iterable);
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            java.util.List<Object> elements = new java.util.ArrayList<>(length);
            for (int index = 0; index < length; index++) elements.add(Array.get(value, index));
            return Flux.fromIterable(elements);
        }
        return Flux.just(value);
    }
}
