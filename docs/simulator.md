# Deterministic simulator

Netz protocol handlers are pure state machines, so hundreds or thousands of
logical peers can share one process and virtual clock. The simulator runs the
same membership, dissemination, repair, and content handlers as production.

The default transport model matches the Kabel WebSocket binding: directed links
are FIFO. Tests may set `:link-fifo? false` to exercise a future unordered
binding. Seeded latency, loss, and duplication remain reproducible.

## Fault schedules

Human-meaningful faults are data:

```clojure
[[:advance 1000]
 [:pause :n2]
 [:isolate :n4]
 [:advance 5000]
 [:heal]
 [:resume :n2]]
```

`netz.sim.schedule-gen` produces test.check schedules that shrink by deleting
faults and shortening time. Seeds still control low-level latency, loss, and
duplication; shrinking a numeric seed would produce an unrelated execution.

## Fault and resource model

- `pause`/`resume` retains volatile memory;
- `crash`/`restart` loses volatile state and unlanded persistence;
- `forget` removes the peer and its durable storage;
- partitions and directed link failures model reachability;
- `:max-inflight` and `:bandwidth-bytes-per-ms` expose queue and burst failures;
- deterministic structural byte estimates agree between JVM and JavaScript;
- a pinned whole-simulation fingerprint detects platform drift.

The 10/100/1,000-peer scale matrix disables traces and asserts delivery,
traffic amplification, and peak in-flight bounds.

The profile apply boundary is also a pure machine. Its schedule tests cover
snapshot/live cuts, per-stream reordering, slow application, item/byte
capacity, exact success values, stale generations, transport exclusivity, and
1,000 independent owners without sockets. Runtime tests then verify the async
effect runner rather than trying to simulate threads.

## Deliberate limits

The pure simulator does not reproduce threads, `core.async` scheduling,
WebSocket implementation queues, kernel buffers, DNS, or cryptographic CPU
cost. Those belong in the smaller runtime and real-socket conformance layers.
Likewise, byte estimates are monotonic accounting units rather than encoded CBOR
lengths; byte-level conformance tests use `netz.wire`.
