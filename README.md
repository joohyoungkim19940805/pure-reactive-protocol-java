# Pure Reactive Protocol Java

PRP provides a transport-neutral, duplex reactive session API for Java 21 and
later. The core uses Java `Flow` and `CompletionStage`; Reactor, Spring, RPC,
WebTransport and other integrations live in separate packages.

The shared [philosophy](https://github.com/joohyoungkim19940805/pure-reactive-protocol/blob/main/PHILOSOPHY.md),
[architecture](https://github.com/joohyoungkim19940805/pure-reactive-protocol/blob/main/ARCHITECTURE.md)
and [protocol](https://github.com/joohyoungkim19940805/pure-reactive-protocol/blob/main/PROTOCOL.md)
define the model. See [compact framing](COMPACT-FRAMING.md) for the opt-in wire
extension and [validation](VALIDATION.md) for tested behavior and limitations.

## Alpha5 carrier correction

The next release coordinate is
`com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`. Its WebTransport carrier
uses the official `tech.kwik:kwik:0.12.1` and `tech.kwik:flupke:0.9.4`
dependencies. Flupke's older transitive Kwik dependency is excluded so the normal
consumer graph contains one official Kwik implementation. There is no separate
compatibility artifact to build or publish, and the PRP JAR contains no replacement
upstream Kwik classes. The sender-scheduling correction is already included in
upstream Kwik 0.12.1.

Unmodified Kwik 0.12.1 dereferences a null peer connection-ID registry while
calculating datagram capacity for a valid zero-length peer CID. The private
`KwikDatagramAccess` helper handles that case at the PRP WebTransport adapter
boundary. It reads the negotiated datagram-frame limit and original sender,
calculates the packet limit with the actual zero-length CID, and queues a real
QUIC DATAGRAM with the original priority and no datagram retransmission policy.
The peer CID and upstream connection object are unchanged. Other CID lengths use
the upstream datagram methods.

The helper caches reflective access to two protected implementation methods,
`getMaxDatagramFrameSize()` and `getSender()`, in the pinned Kwik version. It does
not modify upstream fields, install a Java agent, replace classes, or require an
additional JVM opens flag on the ordinary classpath. These protected methods are
an implementation dependency: a Kwik upgrade requires the datagram-boundary,
packaged-consumer and actual-browser regressions to be repeated. Strong JPMS
encapsulation can deny implementation-package access even before CID detection;
named-module deployment is not supported by these classpath checks, and denied
access fails explicitly. Prefer an upstream
release that fixes the zero-CID lookup and remove the helper after validation.

The WebTransport adapter also handles unknown HTTP/3/GREASE frames according to
RFC 9114 and corrects Flupke 0.9.4's signed single-byte CONNECT capsule reads.
Known and forbidden frame types remain visible to the existing validator;
unknown payloads are skipped without allocating from their declared lengths.
These carrier corrections let the packaged library serve a standard browser
without application-local copies of its transport classes.

These are generic carrier corrections, with no application policy, PRP public
API, wire-format, retry or timeout changes. Unit regressions and packaging checks
do not establish a simultaneous-connection capacity or throughput improvement.
Actual browser interoperability and workload measurements are reported separately
in [VALIDATION.md](VALIDATION.md). Results for earlier patched-Kwik candidates
remain historical evidence and do not validate this adapter implementation.

## Build and release

Use JDK 21 or later to build the Java 21 PRP target. Run `bash gradlew test` for
the configured project tests. Consumers resolve the original Kwik artifact from
Maven Central through PRP's ordinary dependency metadata.

The repository's default branch is `master`. Source changes on that branch do
not publish artifacts. `0.0.1-alpha4` is already released and must not be
overwritten. Publish only the signed
`com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5` artifact with its sources and
metadata; no companion compatibility publication is required. Verify the generated
POM and Gradle module metadata resolve the official Kwik 0.12.1 dependency without
a compatibility artifact or replacement classes. The existing Maven publishing
plugin configures Maven Central and signing; publication requires a separate
release invocation.

## License

PRP's own code is licensed under [Apache License 2.0](LICENSE). Its upstream
Kwik, Flupke and Agent15 dependencies retain their own LGPL license terms; using
the original artifacts does not relicense them as Apache. Their notices and
source obligations still apply when distributing those dependencies. This
adapter workaround does not distribute a modified Kwik implementation.
