# From blob dissemination to a storage backend

Status: design and isolated stable-placement prototype. This document is not a
claim that automatic membership replacement, safe layout migration, or a
production distributed Konserve backend is complete.

## Contract and scope

Netz should support an explicitly administered pool of devices storing immutable,
content-addressed blobs, through the optional Konserve integration. Discovery
finds peers; it does not authorize peers to join a storage pool. The first target
is a small pool, possibly spread across hosts, sites, or independent operators.
Existing placement bounds (32 members, at most 5 replicas) remain in force.

A durable write means W distinct authorized replicas have acknowledged the exact
object at the configured stable-storage boundary, satisfying declared domain
minima. N is the desired replica count, not the number already achieved at write
completion. Report acknowledged durability, desired-placement completion,
currently observed readability, and blocked repair separately.

For immutable objects, one accessible digest-verified copy is enough to read.
At a write's acknowledgement point, W independent durable copies can tolerate
loss of W-1 copies; after verified convergence to N, N-1 copy losses can be
tolerated. These are conditional data-survival statements, not a guarantee of
reachability during partitions, perpetual retention, or recovery after all
copies disappear. Correlated failures, unavailable decryption keys, expired
retention, dishonest storage operators, and incorrect topology declarations
invalidate stronger interpretations. A receipt is evidence of an operation, not
a cryptographic proof that a remote operator will retain bytes forever.

Mutable keys/roots are a separate contract: immutable storage does not provide
linearizable Konserve `assoc`/`update` operations across writers. Signed root
dissemination, conflict-preserving histories, single-writer fencing, or optional
Urd consensus belong above this layer. Do not advertise an arbitrary distributed
Konserve store as a drop-in transactional local-store replacement.

## Lessons from source studies

References use pinned commits so the reasoning remains reproducible. No source
code was copied from these implementations.

* Ceph separates down from out, corroborates reports by failure domain, and can
  suppress automatic removal during whole-subtree outages. Its serving and
  recovery sets differ from desired placement. Historical holders remain useful;
  remapping alone does not authorize deleting them.
  [OSDMonitor](https://github.com/ceph/ceph/blob/844974506e0eb88ddd7d33b3f4f0f2109be17532/src/mon/OSDMonitor.cc#L5312),
  [PeeringState](https://github.com/ceph/ceph/blob/844974506e0eb88ddd7d33b3f4f0f2109be17532/src/osd/PeeringState.cc#L2710).
* Garage's multi-layout write quorums concern mutable metadata. Immutable blocks
  write to current placement and can be recovered from older placements. Its
  offload path checks references and transfers required copies before guarded
  local deletion. Netz needs this distinction, not Garage's entire metadata
  protocol.
  [Block placement](https://github.com/deuxfleurs-org/garage/blob/c85597fd182470df31cb5a335d704cdcb56e69dd/src/block/manager.rs#L350),
  [Offload](https://github.com/deuxfleurs-org/garage/blob/c85597fd182470df31cb5a335d704cdcb56e69dd/src/block/resync.rs#L399).
* Garage retains synchronization progress across layouts and has durable repair
  queues/backoff. Its data and metadata fsync defaults are false; do not equate
  any system's generic quorum acknowledgement with Netz's fsync-qualified
  receipt contract.
  [Layout retirement](https://github.com/deuxfleurs-org/garage/blob/c85597fd182470df31cb5a335d704cdcb56e69dd/src/rpc/layout/history.rs#L92),
  [Repair queue](https://github.com/deuxfleurs-org/garage/blob/c85597fd182470df31cb5a335d704cdcb56e69dd/src/block/resync.rs#L32),
  [Configuration defaults](https://github.com/deuxfleurs-org/garage/blob/c85597fd182470df31cb5a335d704cdcb56e69dd/src/util/config.rs#L18).

## Increment 1: stable placement, without silent wire changes

Profile 1 hashes the authority epoch into every member/object score. An epoch
change therefore reshuffles placement even if membership is unchanged. Preserve
its vectors and interpretation for existing peers.

The isolated `netz.replication.stable-placement` prototype and Python
`netz_protocol.stable_placement` use this canonical CBOR hash input:

```
{"profile": "netz/blob-placement/2", "seed": <64 lowercase hex characters>,
 "object": <object-id>, "member": <full lowercase authority-id>}
```

Rank by descending unsigned SHA-256, with ascending authority ID as tie-breaker;
choose the first bounded rank-ordered combination satisfying the existing domain
policy. The pool seed is public and stable, not an encryption secret. The epoch
still matters for authorization and receipts, but never enters these scores.

The prototype is not connected to signed layouts, retention, controller runtime,
or the default placement API. Activation requires an explicit signed schema
binding the profile and seed to the pool; unknown profiles must fail closed.
No local config switch may reinterpret an already signed profile-1 layout.
Treat seed/profile changes as migrations. Preserve cross-language vectors.

Tests cover epoch/order stability, unconstrained membership movement, domain
constraints, disabled members, and a shared JVM/Python score vector. Minimal
movement is only claimed for unconstrained placement with unchanged policy and
seed. Domain changes/constraints can require more movement. Capacity weighting
and partition-based placement remain later design choices; this prototype does
not add them.

## Increment 2: persisted migration before activation

An isolated CLJC model now lives in `netz.replication.migration`. It records the
trusted transition binding, policy, old holders, targets, transfer attempts,
receipt expiry, and a checkpoint revision. `step` serializes mutations behind an
exact checkpoint-completion barrier (`:busy` means retry, not successful event
delivery). `checkpoint`/`restore` let the test harness model crash boundaries.
The tests explore all six three-replica receipt orders, duplicates, each target's
loss and re-acknowledgement, and crashes around each receipt's checkpoint write.

The model itself is **not a disk-backed runtime or authenticated wire schema**.
It assumes trusted, verified inputs and intact local checkpoint reads. Receipt
authentication, binding layout addresses to actual signed layouts, controller
authority refresh after restart, durable adapter IO, bounded counters, wire
encoding, retention renewal, and serialized deletion are still activation gates.
The only output concerning removal is an advisory set of retirement candidates;
it cannot authorize a physical delete. Grace, protected holders, fresh binding,
unexpired N-target coverage and completed persistence are checked when querying
that set. Authority changes must invalidate outstanding advisory results; never
cache them as delete capabilities. The caller must provide pins and active lease
protections, and refresh current authority even when restoring an old checkpoint.

Use a pure state machine first, with a durable runtime interpreting its effects.
Persist the authorized transition identity (exact old/new layout addresses and
policy), old holders, desired holders, current-attempt receipts, and phase. Bind
evidence to pool, object, layout address, policy, and attempt, not merely an epoch
number. Serialize transitions initially; do not introduce overlapping migrations
until we can prove their rules.

1. Prepare: persist the transition and recovery hints before scheduling copies.
2. Copy: keep old holders usable; send idempotent bounded transfers to new holders.
3. Verify: validate receipts and achieved failure-domain coverage. Old receipts
   remain historical evidence, not new-layout acknowledgements.
4. Converge: conservatively require all N desired holders with qualified coverage
   before allowing retirement. W permits durable write completion, not automatic
   cleanup of the old set while new placement is still under-replicated.
5. Retire: persist explicit eligibility, honor pins/leases/grace, then recheck
   current authority and evidence immediately before deletion. An epoch change,
   timeout, or loss observation alone never grants delete permission.

Keep namespace deletion/GC distinct from removal of a surplus physical replica.
Never delete the last useful old copy to resolve capacity pressure. Fail closed
on forks and unsupported profiles. Removed nodes returning with stale state must
refresh authority before control operations; retained immutable bytes can still
be recovery sources under access policy. Bound historical hints without dropping
the only known recovery source silently. Persisted cutover/write fencing and
stale-client behavior need explicit specification before runtime activation.

### Local checkpoint IO increment

`netz.replication.migration-konserve` now replaces the custom EDN/HMAC file
prototype. It uses Konserve's synced file backend, sealed CBOR, strict
authenticated reads and bounded streaming input. Shared record validation binds
the independently trusted template and revision floor. Konserve owns atomic
replacement and cryptography; its crash simulator exercises the adapter's
persistence barrier. This is not network receipt verification, rollback protection
without an independent floor, controller consensus, or physical deletion.

See [storage consolidation](storage-consolidation.md) for the released dependency,
input bounds, format change and remaining qualification gates, and
[application GC](storage-gc.md) for snapshot and transfer protections.

## Increment 3: bounded repair and authorized replacement

Separate reachability observations, verified storage loss, and membership
authority. Initially require an operator/controller to authorize replacement
from eligible spares. Optional Urd can later replicate that controller state;
ordinary blob transfer does not need Raft.

Repair priority: endangered objects, below W/domain threshold, below N, then
balancing. Bound sender and receiver concurrent work, bytes in flight, and disk
reservations. Persist/reconstruct obligations on restart; back off failures
without starving other objects. Report insufficient disk or eligible domains as
blocked rather than weakening policy. Add disk scrubbing and efficient inventory
reconciliation independently of transfer retries.

Automatic replacement additionally needs partition grace, independent-domain
failure evidence, whole-site outage suppression, replacement-rate limits, and
spare-capacity accounting. A gossip timeout is not sufficient authority.
Retention renewal and persisted writer backpressure are separate release gates;
unbounded asynchronous writes cannot promise a sustained durability floor.

## Validation and release gates

* Portable pure tests: fixed vectors, unchanged profile-1 vectors, deterministic
  placement, domain failures, and movement bounds.
* Simulator: crash/replay after every migration persistence boundary; duplicated,
  reordered, and obsolete receipts; authority changes/forks; stale writers and
  removed peers rejoining; loss of each old/new holder; whole-site partitions.
  Assert no retirement before replacement coverage, and eventual convergence
  only when a valid source, capacity, authority, and delivery become available.
* Capacity/fairness: receiver full midway, reservation cleanup, retry backoff,
  no-spare domains, bounded repair while application writes continue.
* Real file stores: kill processes before/after fsync, rename, receipt persistence,
  transition persistence, and cleanup; corrupt/delete physical pieces; reopen
  controller and stores. Process-kill tests are not power-loss proofs; document
  filesystem/device assumptions and test actual power-loss behavior separately.
* End-to-end demo: three independently restartable file-store peers, N=3/W=2,
  verified write, stop one peer, read from survivors, authorized spare replacement,
  convergence, restart from disk. Add site/party constraints as a second scenario.

Production-backend readiness requires these gates, operational metrics and
recovery documentation, and a declared trust/retention model. The current
prototype is one prerequisite, not completion of that campaign.
