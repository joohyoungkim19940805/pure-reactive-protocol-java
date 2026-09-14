package com.byeolnaerim.prp.spring;

import com.byeolnaerim.prp.profile.rpc.PayloadCodec;
import com.byeolnaerim.prp.profile.rpc.jackson.JacksonJsonPayloadCodec;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import tools.jackson.databind.ObjectMapper;

@Configuration(proxyBeanMethods = false)
@Import(PrpRSocketSpringConfiguration.class)
public class PrpSpringConfiguration {
    @Bean
    public PrpRouteRegistry prpRouteRegistry() {
        return new PrpRouteRegistry();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(name = "prpBlockingExecutor")
    public ExecutorService prpBlockingExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    public PrpRouteInvoker prpRouteInvoker(
        @Qualifier("prpBlockingExecutor") ExecutorService prpBlockingExecutor
    ) {
        return new PrpRouteInvoker(prpBlockingExecutor);
    }

    @Bean
    public PrpAnnotatedRouteRegistrar prpAnnotatedRouteRegistrar(
        ApplicationContext applicationContext,
        PrpRouteRegistry registry
    ) {
        return new PrpAnnotatedRouteRegistrar(applicationContext, registry);
    }

    @Bean
    @ConditionalOnMissingBean(PayloadCodec.class)
    public PayloadCodec prpPayloadCodec(ObjectMapper objectMapper) {
        return new JacksonJsonPayloadCodec(objectMapper);
    }

    @Bean
    public PrpSpringRuntimeAdapter prpSpringRuntimeAdapter(
        PrpRouteRegistry registry,
        PrpRouteInvoker invoker,
        PayloadCodec codec
    ) {
        return new PrpSpringRuntimeAdapter(registry, invoker, codec);
    }
}
