# Kwik compatibility dependency

`com.byeolnaerim:kwik-compat:0.12.1-prp1` is a complete Kwik 0.12.1 distribution
with a minimal zero-length connection-ID correction. It is a separate dependency
of PRP Java alpha5, not a classpath overlay in the PRP JAR. The session, profile,
RPC and framework APIs remain unchanged.

## Scope

The [upstream CID manager](https://github.com/ptrd/kwik/blob/v0.12.1/core/src/main/java/tech/kwik/core/cid/ConnectionIdManager.java)
intentionally keeps `peerCidRegistry` null for a valid zero-length peer CID, but
`getCurrentPeerConnectionIdLength()` dereferences it. This can fail during both
maximum datagram payload calculation and datagram submission. The correction is:

```java
public int getCurrentPeerConnectionIdLength() {
    return peerCidRegistry == null ? 0 : peerCidRegistry.getConnectionIdLength();
}
```

Only this method changes. The peer-selected QUIC CID remains unchanged; a PRP
session ID or WebTransport CONNECT stream ID belongs to a different namespace.
Recalculating the payload limit in the PRP adapter alone would not fix the
underlying lookup because Kwik's `sendDatagram()` invokes the same calculation.
There is no application-specific policy or change to PRP session semantics.

Kwik 0.12.1 already contains the upstream stream-send scheduling correction.
A send request is reserved atomically before its callback is enqueued, preventing
an early callback from leaving buffered stream data without follow-up work.
The previous 0.11-based candidate backported that correction; this candidate uses
the official 0.12.1 implementation and retains its regression coverage.

The build verifies the original 0.12.1 binary, source archive and modified source
checksums, applies `patches/zero-cid-length.patch`, compiles the modified CID
manager for Java 11, and combines it with the unchanged upstream classes. The
modified file preserves the original license header and identifies the change
and date. Both project and published consumers receive the complete implementation.
Runtime dependencies follow upstream 0.12.1: agent15 4.0, HKDF 2.0.0 and SipHash
2.0.1. The agent15 upgrade is part of the carrier dependency update.

## Build and regression

From the repository root with JDK 21 or later:

```sh
bash gradlew :kwik-compat:test :kwik-compat:jar :kwik-compat:sourcesJar
```

The build obtains fixed upstream artifacts through Maven Central and verifies
SHA-256 values declared in `build.gradle`. The main implementation retains the
upstream Java 11 target; the regression tests target Java 21. Tests use the
packaged full JAR, cover zero/nonzero peer CIDs, and retain the controlled
callback-order and consecutive large/chunked stream-write checks. Test-only
Mockito does not enter the published runtime dependencies.

`bash gradlew :kwik-compat:rebuildAllFromSource` additionally compiles all matching
Java sources and creates a separate `source-rebuilt` JAR. The normal published
JAR preserves unrelated upstream class files byte for byte; the full-source build
is available to reproduce and modify the implementation from source without
changing that distribution strategy.

See [VALIDATION.md](../VALIDATION.md) for results and their limits. Earlier
55-test and 100-peer results describe the previous 0.11-based candidate, not this
0.12.1-based artifact. Source changes, unit regressions and a zero-CID fix alone
do not establish a 500-client capacity; packaged browser and workload measurements
must identify the artifact actually tested.

## Consumers and publication

Publish the signed complete `kwik-compat:0.12.1-prp1` JAR, matching sources and
metadata before publishing
`com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`. Do not overwrite alpha4.
PRP excludes Flupke's original `tech.kwik:kwik` dependency and exports this
compatibility artifact instead. This source update does not publish either
artifact to Maven Central.

Do not add a separate original Kwik JAR alongside the compatibility artifact.
Their class packages are intentionally the same; they are alternative complete
implementations. Gradle metadata declares the upstream capability to detect a
conflicting provider. Maven consumers must also remove or exclude any original
Kwik dependency added separately or through another dependency. Validate both the
generated POM and module metadata for a single implementation before release.

Remove this compatibility module when a released upstream Kwik version contains
the scheduling and zero-CID corrections and passes the WebTransport/datagram
checks and PRP integration regressions. Replacing it should change the dependency,
not PRP session semantics or introduce application-specific policy.

## License and source

This module is LGPL-3.0-or-later, separately from PRP's Apache-2.0 code. See
[NOTICE](NOTICE) and the full texts in `src/main/resources/META-INF/licenses`.
The matching source JAR contains the complete original/modified Java sources,
the exact patch, notices, license texts, and the build recipe. Upstream source
provenance is fixed in NOTICE and the build file; no proprietary application
source is needed to reproduce this dependency.
