package com.byeolnaerim.prp.core;

public record NegotiatedCapability(String id, int version, CapabilityDescriptor local, CapabilityDescriptor remote) {}
