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
`com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`. Its WebTransport carrier uses
[`com.byeolnaerim:kwik-compat:0.12.1-prp1`](kwik-compat/README.md), a complete Kwik
0.12.1 distribution with a minimal zero-length connection-ID correction.
The sender-scheduling correction is already part of upstream Kwik 0.12.1;
there is no separate sender backport in this candidate.

The WebTransport adapter also handles unknown HTTP/3/GREASE frames according to
RFC 9114 and corrects Flupke 0.9.4's signed single-byte CONNECT capsule reads.
Known and forbidden frame types remain visible to the existing validator;
unknown payloads are skipped without allocating from their declared lengths.
These carrier corrections let the packaged library serve a standard browser
without application-local copies of its transport classes.

These are generic carrier correctness corrections. They introduce no application
policy, PRP public API, wire-format, retry or timeout changes. The compatibility
artifact is a complete replacement dependency, rather than an extra class in the
PRP JAR. Do not combine it with a separate `tech.kwik:kwik` dependency: both
artifacts contain the same upstream packages. Consumers that add Kwik directly
must remove or exclude the original artifact.

Unmodified Kwik 0.12.1 dereferences a null peer connection-ID registry while
calculating datagram capacity for a valid zero-length peer CID. The compatibility
artifact corrects `ConnectionIdManager.getCurrentPeerConnectionIdLength()` to
return zero in that case. It preserves the peer-selected CID and does not map it
to a PRP session ID or another fabricated identifier. The compatibility module's
[README](kwik-compat/README.md) documents the exact scope and removal criteria.

Unit regressions and packaging checks do not establish a simultaneous-connection
capacity or a throughput improvement. Actual browser interoperability, workload
and runtime measurements must be reported separately in [VALIDATION.md](VALIDATION.md).
Earlier 0.11-based candidate results do not validate this 0.12.1-based artifact.

## Build and release

Use JDK 21 or later to build the Java 21 PRP target. The compatibility dependency
preserves the upstream Java 11 target. Run `bash gradlew test` for the configured
project tests; see the module's README for its focused regression and packaging
checks.

The repository's default branch is `master`. Source changes on that branch do
not publish artifacts. `0.0.1-alpha4` is already released and must not be
overwritten. Publish the signed `kwik-compat:0.12.1-prp1` artifact first, including
its matching sources and license files. After that dependency is available from
Maven Central, publish `pure-reactive-protocol:0.0.1-alpha5`. Verify the generated
POM and Gradle module metadata resolve the compatibility artifact without the
original Kwik JAR before either publication. The existing Maven publishing plugin
configures Maven Central and signing; publication requires a separate release
invocation.

## License

PRP's own code is licensed under [Apache License 2.0](LICENSE). The separate
Kwik compatibility artifact contains modified upstream Kwik code under
LGPL-3.0-or-later; it is not relicensed as Apache. Its [NOTICE](kwik-compat/NOTICE)
records upstream authorship, source provenance, the modification and the matching
source distribution.
