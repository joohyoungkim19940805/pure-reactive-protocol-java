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
