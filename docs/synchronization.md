# Synchronization model

Reliable collaboration needs three distinct recovery ranges. Treating them as
one mechanism either floods too much state or silently loses updates.

## 1. Live dissemination

A publication is signed at its origin, forwarded across interested peers, and
deduplicated by `(origin, epoch, topic, sequence)`. Sequence numbers are scoped
to a topic stream. Subscribers buffer a bounded out-of-order suffix and release
only its contiguous prefix, providing FIFO per publisher and topic without
inventing a total order across independent writers. A missing prefix is repaired
from recent history; overflow or a gap below the repair horizon triggers state
synchronization. `publish!` succeeding means the local bounded runtime accepted
the item; it does not promise that every peer is online or has applied it.

## 2. Recent-message repair

Peers exchange bounded interval summaries. A detected hole still inside a
peer's repair horizon is requested as an exact missing range. This is cheap and
preserves the publication stream, but only while the sender retains the
messages.

## 3. Application state sync

An old gap, a buffer overflow, or a new subscription requires current state,
not an unbounded replay. Netz opens a point-to-point, acknowledged,
backpressured synchronization session and delegates opaque summaries, chunks,
application, and commit to a named profile.

`netz.sync` is the pure session machine for `open`, ordered `chunk`, cumulative
`ack`, atomic `done`, timeout, and `abort`. It limits concurrent sessions,
chunks, and the unacknowledged window. The same handler runs under the simulator
and the overlay runtime.

`netz.apply-owner` models the other half of the boundary: one generation of
one topic/profile. It distinguishes network admission from profile application,
allows one mutation in flight, admits only the selected live transport, and
advances `(origin, epoch, topic)` frontiers only after literal application
success. Snapshot capture waits for an in-flight mutation; commit subsumes the
covered prefix, drains the contiguous suffix, and only then publishes ready.
Capacity or application failure moves the owner to `desynchronized` and emits
one state-sync request instead of dropping an item and continuing.

The current Kabel adapter maps this to `PSyncStrategy`:

| Portable profile operation | Kabel operation |
|---|---|
| Describe local state | `-init-client-state` |
| Produce differential chunks | `-handshake-items` |
| Apply a chunk | `-apply-handshake-item` |
| Apply a live value | `-apply-publish` |

Live values received before the initial handshake completes are bounded and
ordered. A successful synchronization checkpoint must carry the
application-committed stream frontiers represented by its snapshot. Buffered
values at or below those frontiers are subsumed; the newer contiguous suffix is
then released. Overflow marks the topic desynchronized and reruns the handshake;
message repair cannot recover a value that dissemination already marked seen.
The pure owner enforces this distinction; wiring its receipts into the overlay
runtime is an explicit alpha release gate.

## Reference profiles

### `netz/root-log/1`

For Datahike/Konserve with an Urd-selected writer. A summary contains writer
identity, Raft term or fencing token, log index, and content root. A receiver
accepts only a root with a valid publisher binding and a monotonically newer
fence/index. Missing immutable blocks are then fetched by content id.

This is single-writer application state even though transport and storage are
peer-to-peer. `netz.sync.root-log` implements fence/index ordering and accepts
an application-supplied publisher/signature verifier. The optional `test-stack`
gate binds the writer, term and index to a real Urd/Raft simulation and the root
to a real Datahike commit UUID; immutable block closure and the independently
signed portable root envelope remain separate release work.

### `netz/join-semilattice/1`

For Spindel/Yggdrasil state. A summary carries a causal frontier; chunks carry
missing deltas or a snapshot. Application is commutative, associative, and
idempotent, so multiple authorized writers can converge without leader
selection. `netz.sync.join-semilattice` provides an executable set-based
reference model; Spindel/Yggdrasil should replace its full-set summary with
their causal frontier and delta/snapshot representation. The optional stack
gate runs durable Yggdrasil G-Sets through Spindel's actual `PDeltaApply` hook,
including a failed session during partition and convergence after healing.

## Cross-language conformance

`spec/fixtures/sync-open-1.json` fixes one complete outer frame and one opaque
profile summary at the byte level. Clojure/JVM, ClojureScript/Node and Python
all assert the same deterministic CBOR. Python's `interop/python/netz_sync.py`
implements the receiving session boundary and both reference profile shapes;
it is not yet an identity/handshake/WebSocket implementation.

## Encryption

Netz hashes and moves opaque blocks. With storage encryption, the content id
names ciphertext, including its nonce and envelope. The profile should expose
stable signed manifests that reference encrypted blocks and identify the key
epoch. Capability/key exchange must be authenticated separately; discovering
a ciphertext block never grants the ability to decrypt it.

## Programming-model target

The application-facing layer can converge on an asynchronous shape such as:

```text
workspace = connect(configuration)
document = workspace.open(topic, profile)
document.watch(callback)
document.change(operation)
document.status() -> joining | synchronized | desynchronized | synchronizing
```

Clojure and ClojureScript can offer protocols and macros over this interface;
Python and other languages should expose promises, futures, async iterators, or
their native equivalents. This layer belongs above Netz so the same networking
standard can carry Datahike roots, CRDT deltas, or another state model.
