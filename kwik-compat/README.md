# Kwik stream-send compatibility dependency

`com.byeolnaerim:kwik-compat:0.11.1-prp1` is a complete Kwik 0.11 distribution
with the upstream 0.12.1 stream-send scheduling correction backported. It is a
separate dependency of PRP Java alpha5, not a classpath overlay in the PRP JAR.
The session, profile, RPC and framework APIs remain unchanged.

## Scope

The original sender publishes a callback before incrementing its request count.
If the sender executes it immediately, the count can become negative, a needed
follow-up request can be skipped, and an otherwise healthy reliable stream can
stop with bytes still buffered. The official correction atomically reserves the
request before enqueueing the callback. Only
`StreamOutputStreamImpl.scheduleSendStreamFrameRequest(boolean)` changes.
The generated modified file preserves the original license header and identifies
the change and date.

The build verifies the original 0.11 binary, source archive and modified source
checksums, applies the checked-in exact patch, compiles the changed class for
Java 11, and combines it with the unchanged upstream classes. The original
anonymous helper class is retained unchanged. All other upstream class bytes are
preserved. Both project and published consumers receive the complete implementation.
Runtime dependencies remain agent15 3.3, HKDF 2.0.0 and SipHash 2.0.1.

A direct 0.12.1 upgrade is outside this backport. That version's datagram size
calculation dereferences a null peer CID registry on a valid zero-length-CID
browser connection. Avoiding that separate compatibility change keeps this fork
to one proven sender correction.

## Planned follow-up: Kwik 0.12.1 zero-CID correction

Recorded on 2026-11-02 as a future direction only. No implementation, dependency
version, artifact coordinate or publication changes are made by this note. The
current alpha5 candidate continues to use the 0.11-based compatibility artifact.

The planned next carrier evaluation uses Kwik 0.12.1, which already contains the
sender correction, with a minimal generic fix for its zero-CID length lookup.
Both 0.12 and 0.12.1 reproduce an NPE for a zero-length client CID in the public
CID-manager constructor/getter probe; an eight-byte client CID succeeds. The
actual browser reproduction was performed with 0.12.1. This identifies a Kwik
input-handling defect, not a claim that every browser or QUIC connection fails.

The [upstream CID manager](https://github.com/ptrd/kwik/blob/v0.12.1/core/src/main/java/tech/kwik/core/cid/ConnectionIdManager.java)
intentionally keeps `peerCidRegistry` null for a valid zero-length peer CID, but
`getCurrentPeerConnectionIdLength()` dereferences it. The proposed correction is:

```java
public int getCurrentPeerConnectionIdLength() {
    return peerCidRegistry == null ? 0 : peerCidRegistry.getConnectionIdLength();
}
```

Preserve the peer-selected QUIC CID. A fabricated CID, PRP session ID or
WebTransport CONNECT stream ID cannot substitute for it. Recalculating the
payload limit in the PRP adapter alone is insufficient: Kwik's `sendDatagram()`
invokes the same broken calculation internally. Keep the correction in the
generic QUIC implementation, with no application-specific policy or PRP semantic
changes. Prefer a corrected official release when available and validated, then
remove the temporary patch/compatibility dependency.

Before adopting this direction, verify zero/nonzero-CID length and datagram
boundaries, real browser reliable/datagram progress and close/cancel behavior,
Java 21/25 regressions, and the complete packaged consumer dependency and license
metadata. Existing 55-test and 100-peer results in [VALIDATION.md](../VALIDATION.md)
belong to the current 0.11-based candidate; they do not validate this proposed
0.12.1 correction or establish a 500-client capacity.

## Build and regression

From the repository root with JDK 21 or later:

```sh
bash gradlew :kwik-compat:test :kwik-compat:jar :kwik-compat:sourcesJar
```

The build obtains fixed upstream artifacts through Maven Central and verifies
SHA-256 values declared in `build.gradle`. The main implementation retains the
upstream Java 11 target; the regression tests target Java 21. The tests use the
packaged full JAR, control callback execution order on actual Kwik stream/output
objects, and check consecutive large writes and chunked byte ordering. Test-only
Mockito does not enter the published runtime dependencies.

`bash gradlew :kwik-compat:rebuildAllFromSource` additionally compiles all 201
matching Java sources and creates a separate `source-rebuilt` JAR. The normal
published JAR preserves the 253 unrelated upstream class files byte for byte;
the full-source build is available to reproduce and modify the implementation
from source without changing that distribution strategy.

## Consumers and publication

Publish the signed complete JAR, matching sources and metadata before publishing
`com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`. Do not overwrite alpha4.
PRP excludes Flupke's original `tech.kwik:kwik` dependency and exports this
compatibility artifact instead.

Do not add a separate original Kwik JAR alongside the compatibility artifact.
Their class packages are intentionally the same; they are alternative complete
implementations. Gradle metadata declares the upstream capability to detect a
conflicting provider. Maven consumers must also remove or exclude any original
Kwik dependency added separately or through another dependency. Validate both the
generated POM and module metadata for a single implementation before release.

Remove this compatibility module when a released upstream Kwik version contains
the scheduling fix and passes the zero-CID WebTransport/datagram checks and PRP
integration regressions. Replacing it should change the dependency, not PRP
session semantics or introduce application-specific policy.

## License and source

This module is LGPL-3.0-or-later, separately from PRP's Apache-2.0 code. See
[NOTICE](NOTICE) and the full texts in `src/main/resources/META-INF/licenses`.
The matching source JAR contains the complete original/modified Java sources,
the exact patch, notices, license texts, and the build recipe. Upstream source
provenance is fixed in NOTICE and the build file; no proprietary application
source is needed to reproduce this dependency.
