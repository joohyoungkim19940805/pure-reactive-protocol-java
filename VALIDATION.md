# Java alpha5: official Kwik 0.12.1 with a PRP datagram adapter — 2026-11-02

The current candidate is `com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`.
It depends directly on the unchanged official `tech.kwik:kwik:0.12.1` artifact.
The separate compatibility module, replacement Kwik distribution, source patch,
checksummed rebuild recipe and companion publication requirement have been
removed. Only the PRP artifact must be built/signed/published. No external Maven
publication or application deployment was performed by this change. Central was
checked and alpha4 remained the latest released PRP version.

## Scope and implementation

`KwikHttp3ServerConnection` routes datagram size/sending through the private
`KwikDatagramAccess` helper. For a real Kwik connection whose existing CID manager
reports a zero-length peer CID, the helper reads the negotiated frame limit and
original sender through two cached protected-method handles. It calculates the
same packet/frame bound with a CID length of zero and queues the real native
`DatagramFrame` at App encryption level with the original priority and an empty
loss callback. It changes no CID, upstream field or connection object, does not
retransmit a lost datagram and does not substitute a reliable stream. Nonzero CID
and other connection implementations delegate to their original datagram API.

The official JAR remains byte-for-byte unchanged. Its direct zero-CID getter and
`sendDatagram()` still reproduce the upstream NPE in the regression; the PRP
adapter avoids those calls only on that affected path. The official 0.12.1 sender
CAS fix is retained unchanged and its controlled race regressions remain in the
PRP test suite. PRP session semantics, public API and wire framing are unchanged.

The protected-method dependency is pinned to Kwik 0.12.1 and must be reviewed on
upgrade. Ordinary classpath execution requires no extra opens flags. Strong JPMS
encapsulation can reject the implementation-package access even before CID
detection; named-module deployment is not supported by these checks and access
failure is explicit. An officially corrected release remains the removal target
for this private workaround.

## Verification

- Java 25 and Java 21 test runtimes each pass all 62 tests, with no failures,
  errors or skips. JDK 25 compiles the Java 21 target. The suite includes 53
  existing PRP checks, two upstream sender-scheduling regressions and seven
  datagram adapter regressions.
- Actual Kwik construction/negotiation tests cover the original zero-CID NPE,
  unchanged CID identity, peer frame limits 1/2/32/1000/65535, packet limits
  1200/1252, payload-at-limit acceptance, oversize/null rejection, disabled and
  receive-only capability states, native priority dispatch, no loss retransmit,
  no stream substitution and unchanged delegation for other CID/implementations.
  Packet dispatch and unrelated test collaborators are mocked; production
  transport state is not changed by reflection.
- External Gradle-metadata and POM-only consumers both compile for/run on Java 21.
  The POM-only check disables Gradle metadata redirection; this is not a Maven CLI
  invocation. Both resolve a single original Kwik 0.12.1 JAR, agent15 4.0 and no
  compatibility artifact. The PRP JAR contains zero `tech/kwik/` classes.
- The original Kwik JAR checksum is checked and sender/CID classes share that
  original artifact origin. Generated publication metadata hashes match the
  corresponding PRP binary/source artifacts. Consumer public builders and
  nonzero CID identity checks pass.
- The packaged application integration uses three independent actual Chromium
  WebTransport/HTTP3/QUIC connections with full decoding and the current 5000ms
  common combat period. Carrier/session/helper classes load from the PRP JAR;
  sender and CID manager load from the original Kwik JAR, without application
  carrier overrides or replacement upstream classes. All three pass 31.512
  measured seconds: simulation 20.001Hz, median reliable representation 5.046Hz,
  zero errors/stale/gameplay failures; cleanup counters zero and JVM exit zero.
  This application fixture uses a test-only Mockito agent for its mocked
  persistence/engine observations; the adapter itself installs no agent.

A separate public-API-only server probe also passes on both Java 21 and Java 25
with `VMargs=[]`: no Java agent, no add-opens/add-exports, no Mockito and no Hunter
classes. Each run opens three simultaneous real Chromium WebTransport sessions,
echoes 80,000 reliable bytes per peer exactly (240,000 total), echoes three native
datagrams per peer (nine total), and rejects an oversized server datagram for each
peer. It uses the original Kwik JAR with the same checksum below. All three
sessions close, active count returns to zero and the JVM exits zero. This
separately demonstrates that the adapter workaround itself does not depend on
the application fixture's test agent.

The current binary checksums are:

```text
pure-reactive-protocol-0.0.1-alpha5.jar
bcb4cce128ddeab05d1637b478ea88e20923c569d9ce8872dfd1d17303f5e17d
kwik-0.12.1.jar (official, unmodified)
179b19ac36430b723093345df66ab32fb7a10e9d2f209c7bc0beaff6a5641461
```

The current adapter has not repeated the 100/200/300/500 capacity matrix. Those
previous tests used the superseded patched-Kwik candidate, a shared 4-CPU/16GiB
loopback environment, mocked persistence and a protected fixture. They do not
prove capacity, performance improvement or production readiness of this adapter.
All sections below are historical evidence; their compatibility coordinates and
old companion-publication instructions do not apply to the current source.

---

# Historical candidate: separate patched Kwik 0.12.1 distribution — 2026-11-02

This superseded candidate prepared `com.byeolnaerim:pure-reactive-protocol:0.0.1-alpha5`
with `com.byeolnaerim:kwik-compat:0.12.1-prp1`. It superseded the 0.11-based
candidate described below; it does not publish artifacts or change a deployed
application. That compatibility dependency contained official Kwik 0.12.1,
including its original sender-scheduling fix, and one additional correction:
`ConnectionIdManager.getCurrentPeerConnectionIdLength()` returns zero for the
existing null-registry/zero-length-peer-CID state. The peer CID is unchanged.

## Correctness and packaging

- Java 25 and Java 21 test runtimes each pass all 58 tests: 53 PRP tests and five
  complete-packaged-Kwik tests, with no skipped tests or failures. Compilation
  uses JDK 25, targeting Java 21 for PRP/tests and Java 11 for Kwik.
- Two of the three new CID/datagram tests fail on unmodified 0.12.1; all three
  pass on the correction. They preserve zero/nonzero peer CID bytes and exercise the
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

The subsequent isolated capacity checks did not establish a 500-client service
capacity. The following shared-turn runs used real independent QUIC transports
and complete packaged libraries. Server and browser shared the same 4-CPU/16GiB
limit. The load generator received all payloads but validated envelope/owner/ACK
metadata without reconstructing/rendering entities (`headers` mode).

| Target | Measured live | Measured seconds | Simulation Hz | Reliable frame median Hz | Echo p95 ms |
| --- | --- | --- | --- | --- | --- |
| 100 | 100–100 | 71.367 | 19.99 | 5.00 | 27.1 |
| 200 | 200–200 | 97.061 | 19.86 | 4.98 | 363.9 |
| 300 | 300–300 | 64.361 | 19.79 | 4.54 | 3240.3 |
| 500 | Hold not reached | — | — | — | — |

At 100/200, every peer met the original transport/combat progress evidence, with
zero transport errors, stale clients or server invariant violations. The stricter
full scenario additionally required two accepted MOVE executions per peer;
application player collisions prevented that for some peers, so both full-scenario
results remain FAIL. At 300, all connections progressed but up to 16 peers were
stale, echo p95 exceeded three seconds, gameplay rejections occurred and some
combat execution deltas were insufficient (72 peers did not meet the original
combat criteria); it is not a healthy service result.

The 500 ramp after the 300 stage stopped at 330 attempts / 329 distinct JOINs and
ready peers after one opening-handshake failure. A fresh 500-target run stopped
at 310 attempts / 309 JOINs and ready peers, with three echo timeouts and one
opening-handshake failure; the last ramp report had 306 live peers. Neither
reached the 500 hold. Those observations do not define a universal 329/309-peer
limit. Both failed runs cleaned session/stream/connection counters to zero and
exited with JVM status zero.

During a 162.104-second observation window after the fresh ramp started, the
shared cgroup averaged 3.700 CPU cores, with 1287/1595 throttled periods; host
namespace UDP receive-buffer errors increased by 32,105. These are shared/window
pressure measurements, not per-connection cause attribution or whole-run totals.
A failure dump contains 351 writers waiting for send-buffer space. Without the
queued-request/buffer/credit time series, that alone does not establish recurrence
of the old sender race or identify the exact handshake-failure cause.

Earlier free-motion/full-decoder checks were a different workload: 100 peers
progressed at 19.97Hz simulation, 200 progressed at 16.33Hz, and 300 remained live
but failed the stale criterion at 8.96Hz. Those results are preserved separately.
Loopback/mocked persistence, a protected non-retaliating combat fixture, metadata
validation and controlled input scripting do not establish production database,
external-network, continuous-WASD, full-client-rendering or long-run capacity.
The correction fixes the demonstrated library defects; it does not guarantee
500 simultaneous playable users or justify additional PRP semantics changes.

The packaged candidate JAR hashes are:

```text
pure-reactive-protocol-0.0.1-alpha5.jar
84ea505919d412638f50d9f79db28319fbd29846ccd3f912620f775902d90506
kwik-compat-0.12.1-prp1.jar
83802a74aa780b30a73ef7b5f14737d82f1da237783cd57750a86d8412d7914b
```

Historical release plan (no longer used): this candidate would have published
the signed complete compatibility artifact before PRP alpha5.
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

Historical release plan (no longer used): this candidate required a separate
compatibility artifact publication before PRP alpha5. The current adapter
candidate above requires only the PRP artifact publication.

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
