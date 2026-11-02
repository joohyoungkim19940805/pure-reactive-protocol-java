# Java alpha5: Kwik 0.12.1 carrier validation — 2026-11-02

The current source prepares `com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`
with `com.byeolnaerim:kwik-compat:0.12.1-prp1`. This supersedes the 0.11-based
candidate described below; it does not publish artifacts or change a deployed
application. The compatibility dependency now contains official Kwik 0.12.1,
including its original sender-scheduling fix, and one additional correction:
`ConnectionIdManager.getCurrentPeerConnectionIdLength()` returns zero for the
existing null-registry/zero-length-peer-CID state. The peer CID is unchanged.

## Correctness and packaging

- Java 25 and Java 21 test runtimes each pass all 58 tests: 53 PRP tests and five
  complete-packaged-Kwik tests, with no skipped tests or failures. Compilation
  uses JDK 25, targeting Java 21 for PRP/tests and Java 11 for Kwik.
- The three new CID/datagram tests fail twice on unmodified 0.12.1 and all pass
  on the correction. They preserve zero/nonzero peer CID bytes and exercise the
  real `sendDatagram()` path, maximum payload acceptance and oversize rejection.
  The existing two sender-scheduling tests continue to pass on upstream 0.12.1.
- The complete compatibility JAR contains all 270 upstream class files. Only
  `ConnectionIdManager.class` changes; the other 269 retain their upstream bytes.
  The complete source archive retains all 214 Java sources, with the one modified
  file, original license header, modification notice, exact patch and build recipe.
  A complete Java 11 source rebuild also passes.
- Runtime dependencies match upstream 0.12.1: agent15 4.0, HKDF 2.0.0 and SipHash
  2.0.1. The generated PRP metadata excludes Flupke's original Kwik dependency and
  selects `kwik-compat:0.12.1-prp1`; the compatibility POM has no original Kwik JAR
  dependency. Gradle metadata retains the single-provider capability guard.
- External Gradle-metadata and POM-only consumers both compile for and run on
  Java 21 with exactly one complete 0.12.1-based compatibility JAR and agent15
  4.0. Public CID checks preserve lengths and values for zero, eight and twenty
  bytes. Adding original Kwik 0.12.1 explicitly fails with the expected Gradle
  provider conflict. These checks use staged local Maven-layout artifacts, not
  a Maven CLI invocation or a Central publication; the POM-only path disables
  Gradle metadata redirection. POM consumers adding original Kwik separately
  must still exclude it because Maven cannot express Gradle capabilities.

## Actual QUIC checks

The initial packaged smoke run uses the unchanged PRP alpha5 JAR, the complete
0.12.1-based compatibility JAR and agent15 4.0. Startup asserts that the private
PRP carrier classes load from the packaged PRP JAR and both the CID manager and
stream sender load from the same complete compatibility JAR. There are no
application-local PRP carrier overrides or single-class runtime overlays.

Three independent Chromium 154.0.8037.57 WebTransport peers pass 20.979 measured
seconds of the shared-turn workload: simulation 20Hz, median representation
5.005Hz, zero errors/stale clients/gameplay violations, and cleanup counts zero
with JVM exit zero. The loopback fixture uses in-memory persistence; server and
browser share a 4-CPU/16GiB cgroup limit. This smoke run does not establish higher
concurrent capacity. Larger-stage validation is separate from the NPE correction.

The packaged candidate JAR hashes are:

```text
pure-reactive-protocol-0.0.1-alpha5.jar
84ea505919d412638f50d9f79db28319fbd29846ccd3f912620f775902d90506
kwik-compat-0.12.1-prp1.jar
83802a74aa780b30a73ef7b5f14737d82f1da237783cd57750a86d8412d7914b
```

Maven Central publication remains a separate release action: publish the signed
complete compatibility artifact, matching source and metadata before PRP alpha5.
The following 0.11-based results remain historical evidence and do not validate
the new candidate's larger-stage capacity.

---

# Previous Java alpha5 candidate: Kwik 0.11 backport — 2026-11-02

The candidate coordinates are `com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`
and `com.byeolnaerim:kwik-compat:0.11.1-prp1`. Alpha4 remains unchanged. This work
prepares source and local artifacts; it does not publish to Maven Central or
change an application's deployed dependencies.

## Corrections

Kwik 0.11 can execute a newly queued send callback before incrementing its pending
request count. The count can become negative and suppress a needed follow-up
callback, leaving buffered data with no sender work. A subsequent large write
then waits for buffer space despite available peer flow-control credit.

The complete compatibility dependency backports the official Kwik 0.12.1
`scheduleSendStreamFrameRequest` method. It reserves the request with CAS before
enqueueing the callback. This changes one of the original 254 class files; the
other 253 retain their original bytes. The dependency retains Java 11 bytecode
and all 201 matching sources, the exact patch, modified-file notice, LGPL/GPL
texts and a reproducible build recipe. A full-source Java 11 rebuild also passes.
The PRP JAR itself contains no Kwik implementation classes.

The WebTransport adapter additionally ignores unknown/GREASE HTTP/3 frames
according to RFC 9114 while preserving known/forbidden frames for the existing
validator, and reads CONNECT capsule bytes without Flupke 0.9.4's single-byte
sign extension. Unknown frame payloads are skipped without allocation proportional
to an untrusted declared length. There are no application-specific branches,
new retry/timeout policies, public PRP API changes or PRP wire-format changes.

## Regression and packaging checks

| Check | Result |
| --- | --- |
| Java 25 test runtime | 53 PRP tests + 2 packaged Kwik tests, all pass |
| Java 21 test runtime | Same 55 tests, all pass; none skipped |
| Controlled callback-before-registration regression | Original Kwik: 1 failure / 2 tests; complete compat JAR: 2 pass |
| Large/chunked send behavior | Consecutive 80KB and chunked 125KB preserve progress and byte order |
| HTTP/3 carrier helpers | 11 tests including 8 new GREASE, byte preservation, forbidden-frame passthrough, high-byte, zero-length and truncated-input cases |
| Compatibility artifacts | Complete binary/source JARs, correct license/notice, Java 11 target, only one class changed |
| Published metadata model | PRP excludes Flupke's original Kwik and exports the compat dependency; no original Kwik in either normal consumer graph |
| Gradle metadata consumer | Java 21-target compilation and execution pass with exactly one complete Kwik JAR |
| POM-only consumer | Same positive check with Gradle metadata redirection disabled |
| Conflicting original dependency | Gradle rejects simultaneous original/compat Kwik providers through the declared capability |

Compilation used JDK 25, targeting Java 21 for PRP/tests and Java 11 for the
compatibility implementation. The Java 21 installation supplied the second test
runtime. Consumer checks used an external project and the generated JAR/POM/module
files staged in a local Maven-layout repository; an actual Maven CLI invocation
and an external Central publication were not part of these checks. Maven POMs
cannot express the Gradle capability conflict rule, so consumers adding original
Kwik separately must remove or exclude it as described in the README.

## Packaged browser and actual QUIC checks

The library JAR was placed before the validation application's classes. A small
launcher asserted that both WebTransport carrier classes and the PRP session came
from the alpha5 JAR, and the stream output came from the complete compatibility
JAR. No application-local carrier override or single-class scratch overlay was
used for these candidate runs. The TypeScript runtime remained the published
`0.0.1-alpha3` package.

| Actual Chromium WebTransport / HTTP3 / QUIC check | Result |
| --- | --- |
| Stock alpha4 JAR, application carrier overrides disabled | Three peers close before JOIN; preserved as the browser interoperability baseline |
| Packaged alpha5, 3 peers | All live for 31.153 measured seconds; errors/gameplay violations 0; cleanup 0, JVM exit 0 |
| Packaged alpha5, 100 peers | All live for 113.571 measured seconds / 90 samples; errors/stale/gameplay failures 0; cleanup 0, JVM exit 0 |

The 100-peer run measured simulation at 20.019Hz and median representation at
4.992Hz. Each peer had distinct identity/session and independent QUIC transport
with pooling/reconnection disabled. Success required actual skill executions,
projectile creation, autonomous basic attacks, reservation replacement/cancellation,
no-cost admission, common-round recovery and reliable/native/ACK progress; an
admission ACK alone was not success.

These are loopback tests with in-memory persistence and a protected, non-retaliating
combat fixture. The server and browser generator share a 4-CPU/16GiB limit. They
do not measure external-network latency, production database transactions, 100
rendered game scenes, long-running service reliability or a 500-client capacity.
No 200/300/500-peer stage was run as part of this packaged alpha5 validation.
Byte counters are payload counters, not total network wire consumption.

The normal candidate JAR SHA-256 values were:

```text
pure-reactive-protocol-0.0.1-alpha5.jar
84ea505919d412638f50d9f79db28319fbd29846ccd3f912620f775902d90506
kwik-compat-0.11.1-prp1.jar
05bba303632fbbd30dcd33185fead2e8c94d968a56a935d1c82ca4861e670c3b
```

The signed compatibility artifact and matching sources must be published before
the alpha5 PRP artifact can resolve from Central. Source pushes alone do not make
either coordinate available there.

---

# Java alpha4 concurrency validation — 2026-11-01

The shared [PRP philosophy](https://github.com/joohyoungkim19940805/pure-reactive-protocol/blob/main/PHILOSOPHY.md),
[architecture](https://github.com/joohyoungkim19940805/pure-reactive-protocol/blob/main/ARCHITECTURE.md),
and [wire contract](https://github.com/joohyoungkim19940805/pure-reactive-protocol/blob/main/PROTOCOL.md)
describe the protocol model. The Java API uses `Flow` and `CompletionStage` for
host-language demand and asynchronous completion.

The alpha4 concurrency corrections preserve that model:

- Reserve each stream's FIFO write slot under its monitor, then execute write
  continuations outside it. Atomic reassembly accounting avoids taking the
  session monitor during stream cleanup while preserving the aggregate limit.
- Release publisher drain ownership atomically with the queue/demand check so
  accepted items cannot be stranded during an ownership handoff.
- Route publisher failure through the same drain path as other signals, marking
  the publisher terminal before invoking `onError`. Later demand cannot repeat
  that signal, and blocked producers are released.

`StreamLifecycleConcurrencyTest` covers the original monitor inversion,
DATA/COMPLETE ordering, cancellation of a credit wait, and reassembly-budget
reuse. `BoundedPublisherTest` covers concurrent delivery, reentrant callbacks,
completion waiting for demand, single error termination, failure before
subscription, and release of a producer waiting on a full queue.

The final source passes all 45 tests with Java 21 and Java 25 test runtimes.
Compilation used JDK 25 with the existing Java 21 bytecode target. Existing
legacy/compact codec, TCP, RPC, datagram, and WebTransport adapter tests remain
part of that suite; they do not cover every independent cross-language carrier
or production deployment.

These are correctness corrections, with no public API, wire-format, dependency,
or application-specific policy changes. They do not establish higher normal
throughput, lower allocation, or a network capacity guarantee. In particular,
FIFO reservation has an allocation cost. Comparative performance, fault
injection, and long-running interoperability remain separate validation work.
