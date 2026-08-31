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
advances `(origin, epoch, topic)` frontiers only after exact application success
(`{:ok true}` with no `:error` key). Apply operations are fenced by owner
generation. Snapshot attempts have distinct session IDs, capture waits for an
in-flight mutation, receive an owner-minted attempt number, and only the
matching local profile operation may return a committed snapshot frontier. A
network frame is never itself a commit event. Sequence and epoch values stay
inside JavaScript's exact unsigned-integer range so JVM and JS owners cannot
disagree about adjacency.
Commit subsumes the covered prefix, drains the contiguous suffix, and only then
publishes ready. Capacity or application failure moves the owner out of live
service instead of dropping an item and continuing; abort and terminal failure
settle every retained receipt. Closed and failed generations cannot be revived.

The profile adapter is the trust boundary for snapshot frontiers: it returns
only frontiers actually represented by the state it durably committed. The
apply owner validates their shape and topic and correlates them with its local
profile-operation ID. It deliberately does not accept the legacy-looking
`:sync/commit` event, because accepting remote frontier claims would permit a
peer to suppress buffered publications without applying their state.
Publication byte counts and committed-frontier encoded byte counts are
mandatory; item, byte, stream, and duplicate-receipt limits bound retained
state. A direct/v0 correctness failure settles and retires that generation. It
never starts recovery on the connection being retired; a replacement owner
must begin the next sync attempt.

The executable overlay carries the authenticated publication envelope to the
ordered application worker. Only an exact local `{:ok true}` result emits an
`application-committed` receipt, and only that local event advances the
frontier used by a later sender snapshot. A peer cannot inject that event.
`sync/done.frontiers` remains an untrusted claim: a legacy `SyncProfile` commits
its application value but returns an empty frontier. A profile implementing
`CommittedFrontierProfile` receives the claim and must return a locally
validated `{:application ..., :frontiers ...}` receipt; Netz validates its
shape and computes its canonical byte size itself. Echoing the wire value
without binding it to the checkpoint is a bug. Kabel
0.3.117 carries the exact decoded CBOR frame size in receiver-local metadata,
which Netz attaches to delivery for bounded accounting and never forwards.

The current Kabel adapter maps this to `PSyncStrategy`:

| Portable profile operation | Kabel operation |
|---|---|
| Describe local state | `-init-client-state` |
| Produce differential chunks | `-handshake-items` |
| Apply a chunk | `-apply-handshake-item` |
| Apply a live value | `-apply-publish` |

Live values received before the initial handshake completes are bounded and
ordered. A successful synchronization checkpoint may suppress buffered
publications only with application-committed stream frontiers returned by the
local profile receipt. Buffered values at or below those frontiers are
subsumed; the newer contiguous suffix is then released. Overflow marks the
topic desynchronized and reruns the handshake; message repair cannot recover a
value that dissemination already marked seen.

Each Netz-managed Kabel-v0 topic now has one runtime `netz.apply-owner`
generation. The owner begins before the initial handshake, holds overlay live
publications while the Kabel snapshot lane applies, accepts only the matching
snapshot completion, drains the contiguous live suffix, and then publishes
ready. Recovery uses a fresh owner-minted operation ID; slow live application
must finish before snapshot capture starts. Explicit unsubscribe is queued on
the same application lane, so it cannot withdraw routing or Kabel state until
the in-flight mutation has settled and the exact owner generation is closed.
Shutdown closes all remaining owners into an absorbing state.

The current Kabel-v0 adapter serializes recovery on a separate one-slot control
lane. Dirty work retains `topic -> candidate sources`, so a horizon report for
one topic never authorizes another topic's snapshot source. Recovery ownership
continues past subscribe-request admission through handshake-ready and the
successful buffered application drain. Per-topic generations prevent an older
drain from clearing a newer gap. Buffered live values carry both intent and
recovery generation; a stale drain can neither apply nor delete a successor's
suffix. Gap transitions themselves run on the ordered application lane, so a
new gap cannot race an old drain already applying. A named source that has
disconnected is never replaced by Kabel's peer-wide fallback connection.

Recovery has a bounded deadline. Error, closed, exception, or timeout retires
every v0 connection that could complete the failed generation late and leaves
the topic gated. Applications can inspect `(netz.pubsub/recovery-status peer
topic)` and may provide `:on-recovery-failed (fn [topic status] ...)` in their
subscription options to terminally settle or replace the affected profile
owner. Netz does not silently retry an exhausted source forever.

Applications using the overlay may call `netz.core/unsubscribe!`,
`netz.pubsub/unsubscribe!`, or `kabel.pubsub/unsubscribe!`: Kabel dispatches
unsubscribe through the installed Netz transport. Netz serializes subscribe,
explicit unsubscribe, and recovery under one lifecycle owner. It
cancels the topic intent, queues a guaranteed overlay-interest withdrawal, and
then performs Kabel's acknowledged drain with a bounded deadline (30 seconds by
default, or `:recovery-timeout-ms` in subscription options). A recovery can
therefore neither recreate an explicitly removed subscription, apply its old
buffer after a later subscribe, nor publish readiness afterward. Shutdown marks
the owner stopped without closing its semaphore, and new lifecycle calls fail
with `:netz/stopped` without waiting behind an in-flight operation.

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
the optional secure client now supplies identity and a Noise/Kabel WebSocket
carrier. Outbound sync-window, repair, content, and reconnect orchestration are
still required before it is a complete application client.

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
