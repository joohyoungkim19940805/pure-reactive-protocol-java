package com.byeolnaerim.prp.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(PrpRoutes.class)
public @interface PrpRoute {
    String value();
    PrpInteraction interaction() default PrpInteraction.REQUEST_RESPONSE;
}
