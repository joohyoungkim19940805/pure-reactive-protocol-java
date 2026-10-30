# Negotiated compact PRP/1 framing — Java 0.0.1-alpha3

Enable the optional capability with `PrpRuntime.builder().compactFrames(true)`.
The default remains false. The peer must also offer `prp.core.compact-frames`
version 1. Old clients and servers continue using the existing PRP/1 wire format.
HELLO and WELCOME always use the existing bootstrap format, including capability
negotiation. The selected framing applies after the session becomes READY. A
session cannot mix compact and legacy reliable frames after this negotiation.

The Spring adapter binds `prp.compact-frames: true` through
`PrpSpringProperties`. The default is false, and an explicit runtime builder
customizer can override that property. The existing three-argument
`PrpSpringRuntimeAdapter` constructor retains the default legacy behavior.

This is binary encoding, not a replacement for TLS or QUIC encryption.

## Reliable frames

The first byte is `0xc1`. The second byte contains the existing frame kind in its
low five bits, with these presence bits:

| Bit | Meaning |
| --- | --- |
| `0x20` | Compact attributes follow |
| `0x40` | A nonzero header value follows |
| `0x80` | DATA fragmentation flag `0x01` |

The control byte is followed by absolute unsigned LEB128 stream ID and peer
sequence, both preserving the existing unsigned 64-bit range. A present header
value is unsigned LEB128 u32 credit or fragment length. Attributes are optional;
the remaining bytes are the payload. Noncanonical/overflowing integers, invalid
frame shapes and compact HELLO/WELCOME frames are rejected.

Attributes begin with a nonzero u32 count. Each entry contains an unsigned LEB128
tag `(dictionaryId << 1) | required`, followed by u32 value length and raw bytes.
Dictionary ID zero escapes to a u32 UTF-8 identifier length and identifier bytes
before the value length. The version-1 dictionary is fixed:

| ID | Attribute |
| --- | --- |
| 1 | `prp.profile` |
| 2 | `prp.rpc.target` |
| 3 | `prp.rpc.pattern` |
| 4 | `prp.error.code` |
| 5 | `prp.session.id` |

Unknown dictionary IDs are invalid. Unknown application attribute names retain
their exact UTF-8 identifiers via the escape representation. Existing attribute
count, identifier and value bounds still apply. The original logical TLV byte
budget (`7 + UTF-8 ID length + value length` per entry) also applies after
decoding, so dictionary compression cannot bypass memory limits.

Fragment payload budgeting retains the conservative 36-byte core allowance and
the larger of compact and legacy attribute sizes. Logical item, credit and
reassembly limits are unchanged.

## Native datagrams

When compact framing is negotiated, the core prefix is one byte `0xd1` instead
of the four-byte `PRD1` magic. Carrier boundaries still delimit each datagram.
The routed-datagram `PRR1` profile retains its six-byte prefix and negotiated
numeric route ID. This preserves separation between routed and arbitrary raw
datagrams. A datagram with the wrong selected prefix is dropped without failing
the reliable session.

## Byte accounting and fixtures

| Example | Existing bytes | Compact bytes |
| --- | ---: | ---: |
| DATA header, stream and sequence each at most 127, no attributes | 36 | 4 |
| DATA with a five-byte application payload under those conditions | 41 | 9 |
| Native core prefix | 4 | 1 |
| Native core plus routed-datagram profile prefix | 10 | 7 |
| 1,000 DATA messages, stream 1, sequences 1..1,000, each with five payload bytes | 41,000 | 9,873 |
| The same 1,000 messages, each with 1,100 payload bytes | 1,136,000 | 1,104,873 |

Header size grows with stream IDs, sequence and optional fields. These figures
exclude transport framing, encryption, application payload and the one-time
handshake. They are byte savings, not a claim of equivalent throughput or CPU
improvement.

The thousand-message totals are asserted against actual encoder outputs in
`CompactFrameCodecTest` and recorded in
`src/test/resources/compatibility/compact-byte-accounting.json`. Sequence values
128 and higher use two bytes, so the accounting does not assume a fixed four-byte
header for every message.

The identical TypeScript fixture corpus is copied into
`src/test/resources/compatibility/compact-frames-v1.json` and consumed by the Java
codec test. The integration tests record actual runtime carrier frames and cover
compact fragmentation/credit, legacy fallback in both directions, buffered
HELLO, numeric datagram routing, raw datagrams and wrong-mode rejection.

The buffered HELLO ordering fix initializes available carrier capabilities before
subscribing to a replaying carrier. Hunter's previous source override for that
same fix can be removed when it uses this implementation.
