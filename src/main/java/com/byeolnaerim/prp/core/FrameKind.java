package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.error.ProtocolViolationException;

public enum FrameKind {
    HELLO(0x01), WELCOME(0x02), CLOSE(0x03), PING(0x04), PONG(0x05),
    OPEN(0x10), DATA(0x11), DEMAND(0x12), COMPLETE(0x13), CANCEL(0x14),
    ERROR(0x15), SIGNAL(0x16), FRAGMENT(0x17);

    private final int value;

    FrameKind(int value) { this.value = value; }
    public int value() { return value; }

    public static FrameKind fromValue(int value) {
        for (FrameKind kind : values()) if (kind.value == value) return kind;
        throw new ProtocolViolationException("Unknown PRP/1 core frame kind 0x" + Integer.toHexString(value) + ".");
    }
}
