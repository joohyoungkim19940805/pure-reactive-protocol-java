package com.byeolnaerim.prp.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class PrpRouteRegistryTest {
    @Test
    void sameRouteMayDeclareDifferentRpcInteractionsWhenInputTypeMatches() throws Exception {
        Handler bean = new Handler();
        PrpRouteRegistry registry = new PrpRouteRegistry();
        registry.register(descriptor(bean, "same.route", PrpInteraction.REQUEST_RESPONSE, "unary", String.class));
        registry.register(descriptor(bean, "same.route", PrpInteraction.REQUEST_STREAM, "stream", String.class));

        registry.seal();

        assertTrue(registry.sealed());
        assertEquals(2, registry.routes().size());
        assertEquals(PrpInteraction.REQUEST_RESPONSE,
            registry.require("same.route", PrpInteraction.REQUEST_RESPONSE).interaction());
        assertEquals(PrpInteraction.REQUEST_STREAM,
            registry.require("same.route", PrpInteraction.REQUEST_STREAM).interaction());
    }

    @Test
    void sameRpcRouteRejectsDifferentDecodedInputTypesAtStartup() throws Exception {
        Handler bean = new Handler();
        PrpRouteRegistry registry = new PrpRouteRegistry();
        registry.register(descriptor(bean, "same.route", PrpInteraction.REQUEST_RESPONSE, "unary", String.class));
        registry.register(descriptor(bean, "same.route", PrpInteraction.FIRE_AND_FORGET, "integer", Integer.class));

        IllegalStateException error = assertThrows(IllegalStateException.class, registry::seal);
        assertTrue(error.getMessage().contains("different request payload types"));
    }

    @Test
    void synchronousHandlerRunsOnConfiguredVirtualThreadExecutor() throws Exception {
        Handler bean = new Handler();
        Method method = Handler.class.getDeclaredMethod("sync", String.class);
        PrpRouteDescriptor descriptor = new PrpRouteDescriptor(
            "sync.route",
            PrpInteraction.REQUEST_RESPONSE,
            bean,
            method,
            0,
            -1,
            String.class,
            String.class
        );

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            PrpRouteInvoker invoker = new PrpRouteInvoker(executor);
            Object response = invoker.requestResponse(descriptor, "hello", null).block();
            assertEquals("hello", response);
            assertTrue(bean.invokedOnVirtualThread);
        }
    }

    private static PrpRouteDescriptor descriptor(
        Handler bean,
        String route,
        PrpInteraction interaction,
        String methodName,
        Class<?> inputType
    ) throws Exception {
        Method method = Handler.class.getDeclaredMethod(methodName, inputType);
        return new PrpRouteDescriptor(route, interaction, bean, method, 0, -1, inputType, inputType);
    }

    static final class Handler {
        volatile boolean invokedOnVirtualThread;

        String unary(String value) { return value; }
        List<String> stream(String value) { return List.of(value); }
        void integer(Integer value) { }

        String sync(String value) {
            invokedOnVirtualThread = Thread.currentThread().isVirtual();
            return value;
        }
    }
}
