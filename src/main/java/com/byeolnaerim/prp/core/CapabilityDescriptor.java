package com.byeolnaerim.prp.core;

public record CapabilityDescriptor(String id, int minVersion, int maxVersion, byte[] parameters) {
    public CapabilityDescriptor {
        if (id == null || id.isEmpty()) throw new IllegalArgumentException("Capability id must not be empty.");
        PrpText.utf8(id);
        if (minVersion < 0 || minVersion > 0xffff) throw new IllegalArgumentException("Invalid minimum capability version.");
        if (maxVersion < minVersion || maxVersion > 0xffff) throw new IllegalArgumentException("Invalid maximum capability version.");
        parameters = parameters == null ? new byte[0] : parameters.clone();
    }

    @Override
    public byte[] parameters() { return parameters.clone(); }

    public static CapabilityDescriptor version1(String id) {
        return new CapabilityDescriptor(id, 1, 1, new byte[0]);
    }
}
