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
`com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`. It replaces the WebTransport
carrier's Kwik dependency with
[`com.byeolnaerim:kwik-compat:0.11.1-prp1`](kwik-compat/README.md).
The compatibility artifact contains Kwik 0.11 with one upstream sender-scheduling
fix backported from Kwik 0.12.1. A callback that ran before its request was counted
could leave buffered stream data without a pending send request. The backport
counts the request atomically before enqueueing it.

The WebTransport adapter also handles unknown HTTP/3/GREASE frames according to
RFC 9114 and corrects Flupke 0.9.4's signed single-byte CONNECT capsule reads.
Known and forbidden frame types remain visible to the existing validator;
unknown payloads are skipped without allocating from their declared lengths.
These carrier corrections let the packaged library serve a standard browser
without application-local copies of its transport classes.

This is a generic QUIC stream correctness fix. It introduces no application
policy, PRP public API, wire-format, retry or timeout changes. The compatibility
artifact is a complete replacement dependency, rather than an extra class in the
PRP JAR. Do not combine it with a separate `tech.kwik:kwik` dependency: both
artifacts contain the same upstream packages. Consumers that add Kwik directly
must remove or exclude the original artifact.

A direct Kwik 0.12.1 upgrade was also evaluated. In the current WebTransport
integration, a normal browser connection with a zero-length connection ID reached
a null connection-ID registry during datagram size calculation. Retaining the
known-compatible 0.11 carrier with the isolated upstream fix avoids making that
separate compatibility change part of this release. The compatibility module's
[README](kwik-compat/README.md) documents its scope and removal criteria.

Unit regressions and packaging checks do not establish a simultaneous-connection
capacity or a throughput improvement. Actual browser interoperability, workload
and runtime measurements must be reported separately in [VALIDATION.md](VALIDATION.md).

## Build and release

Use JDK 21 or later to build the Java 21 PRP target. The compatibility dependency
preserves the upstream Java 11 target. Run `bash gradlew test` for the configured
project tests; see the module's README for its focused regression and packaging
checks.

The repository's default branch is `master`. Source changes on that branch do
not publish artifacts. `0.0.1-alpha4` is already released and must not be
overwritten. Publish the signed `kwik-compat:0.11.1-prp1` artifact first, including
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
