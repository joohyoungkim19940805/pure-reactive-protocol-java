package com.byeolnaerim.prp.spring;

import com.byeolnaerim.prp.profile.rpc.PayloadCodec;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;

/** Optional RSocket 1.0 compatibility wiring. Application dispatch still uses PrpRouteRegistry. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(name = {"io.rsocket.RSocket", "org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler"})
public class PrpRSocketSpringConfiguration {
    @Bean
    public PrpRSocketRegistryAdapter prpRSocketRegistryAdapter(
        PrpRouteRegistry registry,
        PrpRouteInvoker invoker,
        PayloadCodec codec
    ) {
        return new PrpRSocketRegistryAdapter(registry, invoker, codec);
    }

    @Bean(name = "rsocketMessageHandler")
    @ConditionalOnMissingBean(RSocketMessageHandler.class)
    public RSocketMessageHandler prpRSocketMessageHandler(PrpRSocketRegistryAdapter adapter) {
        return new PrpRSocketBootstrapHandler(adapter, "spring-webflux-rsocket");
    }
}
