package com.byeolnaerim.prp.spring;

import com.byeolnaerim.prp.profile.datagram.RoutedDatagram;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Flow;
import org.reactivestreams.Publisher;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;

public final class PrpAnnotatedRouteRegistrar implements SmartInitializingSingleton {
    private final ApplicationContext applicationContext;
    private final PrpRouteRegistry registry;

    public PrpAnnotatedRouteRegistrar(ApplicationContext applicationContext, PrpRouteRegistry registry) {
        this.applicationContext = java.util.Objects.requireNonNull(applicationContext);
        this.registry = java.util.Objects.requireNonNull(registry);
    }

    @Override
    public void afterSingletonsInstantiated() {
        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Class<?> declaredType;
            try { declaredType = applicationContext.getType(beanName); }
            catch (Throwable ignored) { continue; }
            if (declaredType == null) continue;
            Class<?> targetType = ClassUtils.getUserClass(declaredType);
            Map<String, Method> annotated = annotatedMethods(targetType);
            if (annotated.isEmpty()) continue;

            Object bean = applicationContext.getBean(beanName);
            for (Method method : annotated.values()) {
                Method invocable = AopUtils.selectInvocableMethod(method, bean.getClass());
                ReflectionUtils.makeAccessible(invocable);
                for (PrpRoute route : method.getAnnotationsByType(PrpRoute.class)) {
                    registry.register(descriptor(route, bean, method, invocable));
                }
            }
        }
        registry.seal();
    }

    private static Map<String, Method> annotatedMethods(Class<?> type) {
        Map<String, Method> output = new LinkedHashMap<>();
        ReflectionUtils.doWithMethods(type, method -> {
            if (method.getAnnotationsByType(PrpRoute.class).length == 0) return;
            String key = method.getName() + Arrays.toString(method.getParameterTypes());
            output.putIfAbsent(key, method);
        });
        return output;
    }

    private static PrpRouteDescriptor descriptor(PrpRoute route, Object bean, Method sourceMethod, Method method) {
        if (Modifier.isStatic(sourceMethod.getModifiers())) {
            throw new IllegalStateException("@PrpRoute methods must be instance methods: " + method.toGenericString());
        }

        int payloadIndex = -1;
        int contextIndex = -1;
        for (int index = 0; index < sourceMethod.getParameterCount(); index++) {
            Class<?> parameterType = sourceMethod.getParameterTypes()[index];
            if (PrpContext.class.isAssignableFrom(parameterType)) {
                if (contextIndex >= 0) throw invalid(method, "declares more than one PrpContext parameter");
                contextIndex = index;
                continue;
            }
            if (payloadIndex >= 0) throw invalid(method, "must declare exactly one application payload parameter plus optional PrpContext");
            payloadIndex = index;
        }
        if (payloadIndex < 0) throw invalid(method, "must declare one application payload parameter");

        Class<?> declaredPayloadType = sourceMethod.getParameterTypes()[payloadIndex];
        Class<?> inputType = declaredPayloadType;
        if (route.interaction() == PrpInteraction.REQUEST_CHANNEL) {
            if (!isStreamInput(declaredPayloadType)) {
                throw invalid(method, "REQUEST_CHANNEL payload must be Flux, org.reactivestreams.Publisher, or java.util.concurrent.Flow.Publisher");
            }
            MethodParameter parameter = new MethodParameter(sourceMethod, payloadIndex);
            inputType = ResolvableType.forMethodParameter(parameter).getGeneric(0).resolve(Object.class);
        } else if (route.interaction() == PrpInteraction.DATAGRAM) {
            if (!isDatagramInput(declaredPayloadType)) {
                throw invalid(method, "DATAGRAM payload must be byte[], ByteBuffer, or RoutedDatagram");
            }
            inputType = byte[].class;
        } else if (isStreamInput(declaredPayloadType)) {
            throw invalid(method, route.interaction() + " payload must be a single value, not a Publisher");
        }

        if (route.interaction() == PrpInteraction.REQUEST_RESPONSE
            && (sourceMethod.getReturnType() == void.class || sourceMethod.getReturnType() == Void.class)) {
            throw invalid(method, "REQUEST_RESPONSE must return a response value or asynchronous response value");
        }
        if ((route.interaction() == PrpInteraction.REQUEST_STREAM || route.interaction() == PrpInteraction.REQUEST_CHANNEL)
            && !isStreamOutput(sourceMethod.getReturnType())) {
            throw invalid(method, route.interaction() + " must return Flux, Publisher, Flow.Publisher, Iterable, or an array");
        }

        return new PrpRouteDescriptor(
            route.value().trim(),
            route.interaction(),
            bean,
            method,
            payloadIndex,
            contextIndex,
            inputType,
            declaredPayloadType
        );
    }

    private static boolean isStreamInput(Class<?> type) {
        return Flux.class.isAssignableFrom(type)
            || Publisher.class.isAssignableFrom(type)
            || Flow.Publisher.class.isAssignableFrom(type);
    }

    private static boolean isStreamOutput(Class<?> type) {
        return isStreamInput(type) || Iterable.class.isAssignableFrom(type) || type.isArray();
    }

    private static boolean isDatagramInput(Class<?> type) {
        return type == byte[].class || ByteBuffer.class.isAssignableFrom(type) || RoutedDatagram.class.isAssignableFrom(type);
    }

    private static IllegalStateException invalid(Method method, String reason) {
        return new IllegalStateException("Invalid @PrpRoute method " + method.toGenericString() + ": " + reason + ".");
    }
}
