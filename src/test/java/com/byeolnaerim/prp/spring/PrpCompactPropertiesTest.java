package com.byeolnaerim.prp.spring;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.core.CompactFrameCodec;
import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.profile.rpc.BinaryPayloadCodec;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class PrpCompactPropertiesTest {
    @Test void springPropertyOffersCompactButOldConstructorAndExplicitOverridesKeepTheirContract() {
        var registry = new PrpRouteRegistry(); registry.seal();
        var properties = new PrpSpringProperties();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var invoker = new PrpRouteInvoker(executor);
            var adapter = new PrpSpringRuntimeAdapter(registry, invoker, BinaryPayloadCodec.INSTANCE, properties);
            assertFalse(offered(adapter.createRuntime("memory")));
            properties.setCompactFrames(true);
            assertTrue(offered(adapter.createRuntime("memory")));
            assertFalse(offered(adapter.createRuntime("memory", builder -> builder.compactFrames(false))));
            assertFalse(offered(new PrpSpringRuntimeAdapter(registry, invoker, BinaryPayloadCodec.INSTANCE)
                .createRuntime("memory")));
            assertFalse(offered(new PrpSpringConfiguration()
                .prpSpringRuntimeAdapter(registry, invoker, BinaryPayloadCodec.INSTANCE).createRuntime("memory")));
        }
    }

    private static boolean offered(PrpRuntime runtime) {
        return runtime.capabilities().stream().anyMatch(capability -> capability.id().equals(CompactFrameCodec.CAPABILITY_ID));
    }
}
