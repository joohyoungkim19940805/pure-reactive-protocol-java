package com.byeolnaerim.prp.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Optional PRP core features; old peers continue using their negotiated mode. */
@ConfigurationProperties(prefix = "prp")
public final class PrpSpringProperties {
    private boolean compactFrames;

    public boolean isCompactFrames() { return compactFrames; }
    public void setCompactFrames(boolean compactFrames) { this.compactFrames = compactFrames; }
}
