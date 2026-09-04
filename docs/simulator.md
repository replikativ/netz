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

Portable content-provider routing has a smaller pure resource model alongside
the network simulator. It drives thousands of hostile CID/Authority hints into
the same cache used by the JVM/JavaScript requester and asserts total-CID,
per-CID, and per-provider limits plus exact virtual-clock expiry. Runtime tests
then race 100 callers against the atomic outstanding-request cap and exercise
provider failover without sockets.

The profile apply boundary is also a pure machine. Its schedule tests cover
snapshot/live cuts, per-stream reordering, slow application, item/byte
capacity, bounded duplicate waiters, exact success values, stale generations,
reordered and aborted sync attempts, foreign-topic inputs, transport
exclusivity, and 1,000 independent owners without sockets. Runtime tests then
verify the async effect runner rather than trying to simulate threads.

The secure integration suite supplies the complementary small real-network
case: three peers communicate through http-kit, Kabel CBOR and authenticated
Noise; one peer is stopped and recreated with its durable application state;
then point-to-point synchronization converges both the fenced-root and
join-semilattice profiles. This catches carrier, identity, reconnect and async
ordering mistakes that the virtual network intentionally cannot model.

## Deliberate limits

The pure simulator does not reproduce threads, `core.async` scheduling,
WebSocket implementation queues, kernel buffers, DNS, or cryptographic CPU
cost. Those belong in the smaller runtime and real-socket conformance layers.
Likewise, byte estimates are monotonic accounting units rather than encoded CBOR
lengths; byte-level conformance tests use `netz.wire`.
