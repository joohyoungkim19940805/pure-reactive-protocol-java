package com.byeolnaerim.prp.spring;

import java.lang.reflect.Method;
import java.util.Objects;

public record PrpRouteDescriptor(
    String route,
    PrpInteraction interaction,
    Object bean,
    Method method,
    int payloadParameterIndex,
    int contextParameterIndex,
    Class<?> inputType,
    Class<?> declaredPayloadParameterType
) {
    public PrpRouteDescriptor {
        Objects.requireNonNull(route, "route");
        Objects.requireNonNull(interaction, "interaction");
        Objects.requireNonNull(bean, "bean");
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(inputType, "inputType");
        Objects.requireNonNull(declaredPayloadParameterType, "declaredPayloadParameterType");
    }
}
