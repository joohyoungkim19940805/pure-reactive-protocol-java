package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.error.ProtocolViolationException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public final class PrpText {
    private PrpText() {}

    public static byte[] utf8(String value) {
        if (value == null) throw new IllegalArgumentException("Text value must not be null.");
        try {
            var encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
            ByteBuffer encoded = encoder.encode(CharBuffer.wrap(value));
            byte[] output = new byte[encoded.remaining()];
            encoded.get(output);
            return output;
        } catch (CharacterCodingException cause) {
            throw new IllegalArgumentException("String is not well-formed Unicode.", cause);
        }
    }

    public static String strictText(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(value == null ? new byte[0] : value)).toString();
        } catch (CharacterCodingException cause) {
            throw new ProtocolViolationException("Value is not valid UTF-8.", cause);
        }
    }
}
