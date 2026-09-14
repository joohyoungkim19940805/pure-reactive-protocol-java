package com.byeolnaerim.prp;

import com.byeolnaerim.prp.core.PrpText;

public record ProtocolAttribute(String id, byte[] value, boolean required) {
    public ProtocolAttribute {
        if (id == null || id.isEmpty()) throw new IllegalArgumentException("Attribute id must not be empty.");
        PrpText.utf8(id);
        value = value == null ? new byte[0] : value.clone();
    }

    @Override
    public byte[] value() {
        return value.clone();
    }

    public static ProtocolAttribute of(String id, byte[] value) {
        return new ProtocolAttribute(id, value, false);
    }

    public static ProtocolAttribute required(String id, byte[] value) {
        return new ProtocolAttribute(id, value, true);
    }

    public static ProtocolAttribute text(String id, String value) {
        return new ProtocolAttribute(id, PrpText.utf8(value), false);
    }

    public static ProtocolAttribute requiredText(String id, String value) {
        return new ProtocolAttribute(id, PrpText.utf8(value), true);
    }

    public String text() {
        return PrpText.strictText(value);
    }
}
