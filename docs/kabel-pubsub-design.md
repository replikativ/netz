# Kabel pub/sub lifecycle design

Status: proposed design, before implementation. This document defines the
correctness boundary between Kabel's legacy `PSyncStrategy` pub/sub API and
Netz's portable dissemination and synchronization protocols.

## Decision

Do not replace Kabel's entire pub/sub middleware with one global dispatcher.
Keep independent control traffic independent, but route every frame that can
affect one subscription through one bounded, sequential per-subscription lane.

For the legacy protocol, register/capture live publications before asking the
strategy to produce its snapshot. While the snapshot is being applied, hold
live publications behind a lifecycle barrier. Commit the snapshot, drain the
held live suffix in arrival order, and only then report the subscription ready.

This restores the useful Replikativ behavior without making Kabel responsible
for CRDT, database, or global ordering semantics.

## Why FIFO transport is insufficient

Kabel places messages onto a FIFO connection channel, and WebSocket preserves
frame order. The current middleware then uses `core.async/pub` to split frames
by `:type` into separately buffered channels with independent go loops.
Consequently, FIFO wire arrival does not imply FIFO application:

```text
wire:       handshake-data(0), batch-complete(0), publish(1), complete
consumers:  data loop          batch loop          live loop   complete loop
effects:                         ACK may run first; publish/complete may overtake apply
```

A REPL schedule with one delayed data consumer observed `complete` before the
earlier data effect. The polling loop for `batch-complete` makes this worse: it
waits for at most one second, then applies and acknowledges whatever prefix is
present even when the declared item count was not reached.

There is a second, producer-side race. `handle-subscription!` currently calls
`-handshake-items` before adding the connection to the subscriber set. A write
between those two operations can be absent from both the produced snapshot and
the live stream.

## Required semantics

### Guarantees

For one subscription session, Kabel MUST provide:

1. No publication accepted into Kabel after live capture begins is silently
   lost.
2. Snapshot items are applied sequentially in batch/index order.
3. A batch is acknowledged only after every declared item has explicitly
   succeeded. A strategy result such as `{:error e}` is failure, not a
   successful channel take.
4. Snapshot production has a separate explicit terminal success/error result;
   an item channel closing is not by itself proof of success.
5. Live publications are not applied until the snapshot commits.
6. Buffered live publications are then applied sequentially in arrival order.
7. `ready` becomes visible only after snapshot commit and the finite
   pre-completion-marker suffix has applied.
8. Failure, cancellation, timeout, or overflow becomes visible to the caller;
   none may be reported as successful synchronization.
9. Kabel-controlled queues and outstanding work are bounded by both item count
   and encoded byte size.

Kabel MUST NOT promise:

- a total order across topics, connections, or independent publishers;
- exactly-once delivery across reconnects;
- durable delivery after process failure;
- a consistent snapshot cut for an arbitrary strategy; or
- convergence for non-idempotent application operations.

Netz adds authenticated origin streams identified by
`(origin, epoch, topic, sequence)`, recent-message repair, and profile-level
state synchronization. Those are not properties of legacy Kabel pub/sub.

### The snapshot cut

The legacy strategy API cannot atomically create a snapshot and a following
change-log cursor. Its strongest safe generic sequence is:

```text
authorize -> capture live -> produce snapshot -> apply snapshot
          -> commit -> drain captured live -> ready/live
```

This sequence is correct only when the source and strategy satisfy a **cover
property**: every accepted mutation is either represented by the snapshot or
linearized into the captured publication stream. Kabel can order publications;
it cannot infer whether an arbitrary store mutation is covered. A strategy that
publishes asynchronously after mutating storage must ensure that every mutation
which belongs to the ready checkpoint is represented in the snapshot or queued
as a publication before the snapshot completion marker. A publication emitted
later is ordinary live traffic and cannot retroactively strengthen readiness.

A mutation concurrent with snapshot production may occur in both the snapshot
and captured suffix. Therefore a legacy strategy MUST also be overlap-safe:
reapplying an update represented by the snapshot must converge to the same final
state. Duplicate observer callbacks and storage hooks are still possible unless
the strategy suppresses them; legacy mode does not promise exactly-once side
effects.

This is compatible with the existing important strategies:

- Replikativ's built-in downstream operations are designed to merge
  idempotently. Spindel has this property only when its user-supplied
  `merge-fn`/`apply-delta-fn` obey the advertised CRDT laws; an arbitrary
  function does not become convergent because Kabel transports it.
- Konserve sync mostly applies `assoc`/`dissoc` by key; content-addressed values
  are naturally idempotent at the final-state level. Mutable pointers still
  require snapshot-before-live application so an older snapshot cannot
  overwrite a newer live value. Timestamp-only differential sync does not in
  general establish the cover property across unsynchronized clocks.
- Datahike's store sync deliberately emits immutable nodes before its mutable
  database head and configures mutable values to be sent. `ready` must follow
  all of those writes.
- Urd's `ConsensusPubSubStrategy` has an empty snapshot and transitions
  immediately to live; Raft safety remains the consensus protocol's
  responsibility.

A strategy that cannot tolerate overlap needs a stronger profile operation:
create `(snapshot, frontier)` atomically, then emit only changes strictly above
that frontier. Netz sync profiles expose that frontier/checkpoint boundary.

## Per-subscription state machine

There are two related owners:

- a connection-scoped sender/session lane for handshake flow control; and
- one local receiver/apply lane per subscription/profile instance, shared by
  every selected inbound transport and origin.

The receiver/apply lane has this lifecycle:

```text
requested -> joining -> draining -> live -> closed
                  |          |        |
                  +----------+--------+-> failed
```

The session and apply lanes together own:

- the session/request identifier and topic;
- the strategy and callbacks;
- the next expected batch/chunk index;
- the current batch and declared item/byte count;
- the snapshot producer's independent terminal result;
- the bounded live hold-back queue;
- completion promises for `opened`, `ready`, and `closed`; and
- cancellation/failure state.

Events that enter the apply lane are snapshot data/batches, snapshot completion,
inbound live publication, abort, unsubscribe, and connection close. The lane
awaits and normalizes each strategy result before processing the next effect.
Thrown exceptions, closed-without-result channels, timeout, and explicit error
values fail the session; only the strategy's documented success shape permits
an ACK. Callback failure is contained to the subscription and marks it
failed/desynchronized.

Snapshot production follows the same rule. A checked source has an item stream
and a separate single-assignment completion carrying `{:ok true}` or
`{:error e}`. The sender emits the snapshot completion marker only after it has
drained the source, observed explicit producer success, transmitted every final
batch, and received the receiver's ACK for every batch. Producer exception,
cancellation, error result, completion without a result, or an unacknowledged
batch fails the session; a clean-looking prefix must never become a successful
snapshot.

The ordered snapshot completion event is the finite cut marker. The lane drains
only publications that precede that marker, settles `ready`, changes to `live`,
and then processes later publications normally. Continuous post-marker traffic
therefore cannot starve readiness.

Subscribe requests, authorization, ACK routing, metrics, and unrelated Kabel
traffic may retain separate control-plane handlers. A small router reads the
FIFO connection and uses nonblocking admission to send lifecycle frames to the
appropriate lane. A full lane fails immediately; the router must never park and
recreate global head-of-line blocking. In version one, a slow Datahike snapshot
on topic A therefore need not stop Raft traffic or a live subscription on topic
B.

### Overflow and backpressure

The lane is bounded in messages and bytes. When it cannot accept another live
publication it MUST fail the session rather than discard the oldest item.

For the version-zero direct protocol, where no session-scoped abort exists,
closing the connection and resubscribing is safer than claiming success. This
also closes unrelated topics, so deployments that require Urd traffic to remain
available during bulk sync should use a separate connection until version-one
session isolation exists. For Netz, overflow marks the topic `desynchronized`
and opens a new state-sync session. Recent-message repair alone is insufficient
if dissemination already marked the dropped sequence as seen.

The bound covers Kabel-owned framing, lane queues, and outstanding sends. It
does not bound a strategy that materializes its whole snapshot internally.
Version one additionally requires frame/item size checks before decoding or
allocation and a streaming-strategy conformance rule.

## Legacy protocol correction

The smallest safe correction to the existing Kabel wire protocol is:

1. Reject a second active subscription to the same topic locally, before
   overwriting state or sending it. A server receiving a duplicate from a
   remote/malformed peer retires the version-zero connection because its
   topic-only errors cannot identify which session failed.
2. Add the authorized subscriber to live capture before invoking
   `-handshake-items`.
3. Route that topic's handshake data, batch boundary, completion, and live
   publishes through one lifecycle lane.
4. Register a batch ACK waiter before sending the batch boundary, not after.
5. Require exact batch index and item count; never poll and acknowledge a
   partial batch.
6. Hold live publications while joining and drain them before signaling ready.
7. On handshake failure, remove the subscriber and fail local readiness.
8. Replace the ambiguous producer-close convention with a checked source whose
   item stream and terminal success/error result are distinct. Do not send
   `handshake-complete` after an unconfirmed close.
9. Make `subscribe!`'s completion contract truthful.

The existing wire format has no session identifier on handshake frames, so
version-zero can support only one active session per connection/topic. Delayed
frames from a failed, cancelled, or unsubscribed session cannot be distinguished
from a new one. Consequently, an incomplete session MUST retire its connection,
and unsubscribe/resubscribe of the same topic MUST either replace the connection
or complete an explicit drain rule before reuse. This is a compatibility repair,
not the language-neutral standard.

The repaired Kabel v0 uses the second option for an orderly unsubscribe:
`unsubscribe(id, topics)` is answered by `unsubscribe-ack(id, topics)`, the
server sends that marker only after it has stopped capture, and the client
routes the marker behind earlier frames in each topic's lifecycle lane before
removing local state. An unsubscribe that races an active snapshot cannot
establish this drain and therefore retires the connection. Failure, overflow,
callback exception, and unconfirmed producer completion likewise retire the v0
connection; no later frame may revive its readiness generation.

Existing `PSyncStrategy/-handshake-items` exposes only an item channel. A
compatibility adapter may treat close as success for explicitly trusted legacy
strategies, but that adapter cannot make producer failure observable and is not
standards-conformant. The known stack strategies should migrate to the checked
source before Kabel claims the strong readiness guarantee.

### API result

The portable version-one API should expose a subscription handle rather than
overload one return value:

```text
subscription = await node.subscribe(topic, profile)
await subscription.opened   # authorized and accepted
await subscription.ready    # initial state and captured suffix applied
await subscription.close()
subscription.status         # joining | live | desynchronized | failed | closed
```

For Clojure, these completions may be channels; for JavaScript promises; for
Python awaitables. Multi-topic subscription returns per-topic handles plus an
aggregate completion whose failure policy is explicit.

Kabel version zero cannot expose that exact lifecycle: `handshake-complete`
currently precedes the server's multi-topic `subscribe-ack`, while `subscribe!`
returns as soon as the request is enqueued. Keep that return shape for existing
callers and add `subscribe-session!` with at least per-topic `ready` and
`failed/closed` completions. Version zero cannot independently promise `opened`
without moving or adding a wire acknowledgement; version one can.

## Portable version-one boundary

Do not standardize `PSyncStrategy` method names or EDN values. Kabel should be
the ordered, bounded duplex transport implementation. Netz should define the
portable publication and synchronization messages already described in
`spec/netz-draft-00.md`:

```text
sync/open  {session, topic, profile, summary}
sync/chunk {session, index, data}
sync/ack   {session, through}
sync/done  {session, count, checkpoint, frontiers}
sync/abort {session, code}
```

Version one should add an explicit receiver `sync/committed` response, or define
an ACK of `done`, so the sender can retire session state only after the receiver
has committed. Every frame is correlated by a random session identifier.
Chunks contain their payload as one bounded vector/byte string rather than
separate unindexed item frames followed by an item count.

The checkpoint/frontiers make the cut exact for profiles that can provide one:
buffered publications at or below the committed frontier are subsumed; the
contiguous suffix above it is released. A legacy overlap-safe adapter can use an
opaque checkpoint and replay its entire captured suffix.

## Direct pub/sub and mesh dissemination

Kabel direct fan-out and Netz mesh dissemination must not remain two independent
inbound application paths. Kabel now exposes its peer/topic application
serializer and Netz uses it for overlay delivery:

```text
Kabel direct publish ----+
                         +--> subscription lane --> profile.apply_live
Netz ordered stream -----+
```

An installation chooses which live transport owns a topic. Netz installs an
inbound hook that explicitly rejects Kabel v0 direct live frames, while keeping
the point-to-point snapshot protocol. Accepting both without a shared
origin/sequence identity would risk duplicates, divergent ordering, and
subscribers that see only half the network.

For mesh delivery, FIFO means per `(origin, epoch, topic)` after gap repair. No
meaningful total order exists across origins. CRDT profiles merge concurrent
streams; a fenced single-writer profile rejects the wrong writer/term/index.

## Test plan

### Pure model and simulator

- Generate every placement of a publication around `capture` and `snapshot`,
  and mutations under the cover property; assert that capture-before-snapshot
  has no lost-update schedule.
- Generate permutations of snapshot, live, completion, abort, and disconnect;
  assert snapshot effects precede live effects or the session fails.
- Model bounded queues and slow consumers; assert overflow never yields `ready`.
- Return explicit strategy error values, close result channels without a value,
  and throw callbacks; assert no ACK or readiness is emitted.
- Model duplicate, missing, reordered, and stale batch/session indices.
- Run thousands of topics and sessions without sockets, with deterministic
  seeds and shrinkable failure traces, as Urd does for consensus.

### Kabel JVM and ClojureScript

- Reproduce cross-type overtaking with a deliberately slow strategy.
- Publish during snapshot creation, snapshot transfer, snapshot application,
  completion, and captured-suffix draining.
- Verify ACK registration before send using an immediately responding peer.
- Verify two topics progress independently when one strategy blocks.
- Verify unsubscribe, disconnect, timeout, callback exception, and queue/byte
  overflow settle every completion exactly once.
- Verify progress/inactivity timeouts permit arbitrarily long healthy transfers
  and fail stalled transfers; never use total snapshot duration as the timeout.
- Run old/new compatibility tests for the version-zero repair.

### Stack and interoperability

- Konserve/Datahike: immutable nodes arrive before the head; a concurrent newer
  head survives initial sync.
- Spindel/Yggdrasil: multiwriter deltas converge across partition/rejoin.
- Urd: `ConsensusPubSubStrategy` traffic remains responsive during an
  unrelated large sync.
- Netz: one topic has exactly one live inbound owner and overflow triggers state
  synchronization.
- Python/CBOR: an independent client passes canonical frame fixtures, malformed
  input cases, session lifecycle tests, and a black-box sync against JVM/JS.

## Rollout

1. Add the pure subscription machine and exhaustive schedule tests.
2. Apply the narrow version-zero Kabel correction and downstream regression
   tests; do not merge the current global-dispatcher prototype.
3. **Implemented for the v0 adapter:** route Netz overlay application through
   Kabel's peer/topic serializer and reject the identity-less direct live path.
4. Finalize the version-one `sync/committed`/done-ACK semantics and canonical
   CBOR fixtures.
5. Implement the independent Python transport/sync client.
6. Exercise the Datahike/Urd and Spindel/Yggdrasil vertical simulations before
   publishing Kabel and Netz alpha releases.

## Prior art

The design follows established snapshot-plus-stream practice:

- Replikativ subscribed a large live buffer before starting the initial
  handshake, completed the handshake, and then drained live publications.
- Redis full synchronization produces a snapshot while buffering new writes,
  transfers the snapshot, and then sends the buffered command stream:
  <https://redis.io/docs/latest/operate/oss_and_stack/management/replication/>.
- PostgreSQL logical slots export a snapshot representing the state immediately
  before the following ordered change stream:
  <https://www.postgresql.org/docs/current/logicaldecoding-explanation.html>.
- AT Protocol queues repository events while a repository is `in-progress`,
  commits the full resync, processes the queue, and only then marks it
  `synchronized`: <https://atproto.com/specs/sync>.

These systems differ in topology and consistency model, but all make the
snapshot/stream cut explicit. FIFO transport alone is never the cut.
