# Konserve-backed object exchange

Status: design for the post-alpha object-exchange profile. This document maps
the current Netz blob work to the replikativ stack; it does not add a new wire
compatibility claim.

## Goal

Netz should make an immutable value durable, discoverable and recoverable
across authorized peers without turning an application's storage model into a
Netz model. In the replikativ stack, Konserve is the common storage interface,
so the first integration should work with a connected Konserve store rather
than require each application to implement a second blob repository.

The useful contract is:

```text
application lineage/catalog
  (Datahike, Geschichte, Yggdrasil, continuation catalog)
            |
            | publishes a root only after object durability
            v
Konserve object adapter
  (opaque key, encoded bytes, metadata/capability)
            |
            v
Netz object exchange
  (authorization, placement, transfer, receipt, repair, deletion)
            |
            v
Konserve stores on authorized peers
  (file, IndexedDB, S3, memory for tests, or another backend)
```

This is deliberately not a transparent distributed map. Konserve operations
include mutable nested updates, atomic multi-key batches, one-shot binary
streams and backend-specific durability. Giving all of those an implicit
quorum meaning would hide failures and make ordinary local updates acquire
surprising network semantics.

The implementation now uses that boundary in both directions: a connected
Konserve store backs content-addressed transfer pieces/install markers and the
receiver's monotone lifecycle journal. `netz.replication.lifecycle-runtime`
turns that journal into the asynchronous offer authorizer expected by the
Kabel/Noise blob runtime. This makes Konserve the common storage integration
surface without assigning network conflict semantics to arbitrary Konserve
keys. The same adapter now exposes sorted installed-manifest pages and a
full-object scrub operation, which are the local evidence a later tiered
read-repair controller needs. The runtime also supports an explicit
authenticated provider fetch backed by that installed-manifest operation. It
does not intercept `konserve/get`: applications or a later tiered adapter
select a provider and invoke retrieval deliberately.

## The object/ref split

A publication has two phases.

1. Store immutable objects and obtain the required durable receipts.
2. Publish a small authenticated reference which makes those objects reachable.

The reference may be a signed Netz root record, a fenced Datahike root, a
Geschichte ref transaction, a Yggdrasil fork descriptor, or a continuation
catalog fact. It is never inferred from a successful blob upload. Conversely,
a pub/sub notification is an acceleration hint: a peer that missed it must be
able to reconcile the reference stream and then fetch its immutable closure.

This is the same useful separation as a Git push: pack data can arrive before
the ref transaction which publishes it. It also prevents a catalog from naming
data that existed only in a failed writer's volatile store.

## Identities: do not confuse four names

One value can have four related but different identifiers:

- an **application key**, such as a Datahike store-ref UUID, a Geschichte
  payload ID, a drive SHA-256 string, or a pretrained Hasch UUID;
- a **wire object address**, which verifies the exact opaque byte sequence
  transferred by a negotiated Netz address profile;
- a **transfer manifest address**, which identifies the bounded resume layout;
- a **backend storage key**, which is private to a Konserve backend or
  capability scheme.

The adapter records the authenticated mapping between the application key and
wire object address. Netz must not assume that a Hasch UUID, Git object ID,
Konserve filename, CID and profile-1 SHA-256 address are interchangeable.

For the current `netz/blob-replication/1` profile, the wire object address is
the domain-separated SHA-256 of the transferred bytes. This remains a sound
integrity anchor. An application key can be carried by the signed catalog or
root closure; it should not be inserted into the blob manifest without a
versioned schema and an authorization rule.

## Chunking and materialization

Netz transfer pieces provide bounded frames, backpressure and restart resume.
They are not the application's permanent chunk format.

- Geschichte already stores large Git pack streams as ordered 4 MiB
  content-addressed chunks and uses content-defined chunking for large files.
- pretrained-rstr stores immutable tensor chunks whose storage geometry is
  intentionally different from GPU page geometry.
- Datahike and persistent-sorted-set already expose a graph of immutable
  store-ref targets.

An adapter should therefore transfer each existing immutable application
object as one logical Netz object. Netz may segment it during transit, but the
receiver installs the verified logical bytes at the application's Konserve
key. It should not persist a second application-visible chunk tree.

Large streams need a bounded implementation: hash and segment while copying
the one-shot input into durable staging, or reopen an already committed
Konserve value through `bget`. The sender must not read a multi-gigabyte value
fully into heap. The current in-memory `blob/split` helper is a fixture and
small-value API, not the eventual streaming adapter.

For a collection such as a Git pack manifest or a Datahike index closure, the
application supplies an ordered, bounded closure descriptor over its existing
objects. Netz transfers missing objects in parallel under global byte and item
limits. This avoids the sequential remote object walk which makes naive
distributed Git object stores slow.

## Mutable-head policies

Object exchange does not select one universal write policy. Profiles can use:

### Signed single writer

One configured Authority signs a monotonically versioned, hash-chained root.
Urd may select and fence that writer. This is the simplest first production
profile for a Datahike server, a canonical Geschichte branch, or a continuation
catalog writer.

### Compare-and-swap/fenced writer

Several eligible writers may propose a successor, but publication linearizes
through a CAS service or an Urd/Raft fence. Payload durability precedes the
CAS. This is appropriate for centrally served Git-compatible repositories that
need read-after-write consistency from every serving replica.

### Multi-head fork set

Concurrent signed heads remain separate DAG tips. Yggdrasil/Spindel/Simmis
expose the forks and an authorized proposal or merge settles them. This is the
natural peer-to-peer collaboration profile; pretending it is last-write-wins
would discard useful causality.

LWW is acceptable only for explicitly disposable metadata. Clock comparison
does not establish durable write authority and should not be the default for
database or repository roots.

## Stack use cases

### Geschichte and Dvergr

A Geschichte push/import already separates guarded immutable payload writes
from the Datahike transaction that publishes their store refs. Netz can place
the payloads first, record receipts, and then disseminate the signed/fenced
Geschichte or Datahike head. A receiving Dvergr worker materializes an ordinary
local Geschichte repository and continues using its range-efficient pack
chunks and Git-compatible operations.

For agent forks, immutable prefixes are shared. A Simmis proposal can publish
a multi-head fork descriptor and transfer only new payloads; adoption advances
the authorized canonical head. This gives cheap throwaway agent repositories
without requiring three always-live repository processes.

### Spindel and Yggdrasil

Spindel's distributed workspace peer already gates exposure of a workspace on
every local system reaching the pinned head. Netz can replace the assumption
that a reliable live pub/sub stream delivered every update: descriptor and head
notifications trigger reconciliation, object receipts make local availability
explicit, and the workspace is re-seated only after the closure is present.

CRDT deltas remain a Yggdrasil synchronization profile. Large immutable values
referenced by the CRDT use the object plane rather than being copied through
the delta stream.

### Simmis drive and collaboration

Dvergr/Simmis drive blobs are already opaque bytes under a SHA-256 application
key in a configurable Konserve store. They are the most direct demonstration:
store locally, replicate to a laptop/home device/remote peer under a privacy
domain, publish the drive-tree update after the requested receipts, then prove
read repair after one device is lost.

Private deployments encrypt before Netz addressing. Authorization, finite
leases and deletion receipts are required; global plaintext deduplication is
not an acceptable privacy default.

### pretrained-rstr

Continuation tensor chunks are immutable and cataloged separately from their
placement. A worker can request a chunk from the cheapest observed peer, verify
and install it in a local Konserve tier, and fall back to object storage or
recomputation. A ready replica fact is published only after verification.

The Netz receipt and inventory plane can replace ad-hoc peer-copy evidence,
while pretrained retains model compatibility, scheduling, GPU page placement
and recomputation policy. Approximate continuation matching never becomes an
object-integrity rule.

## Lessons from Git-at-scale systems

[Cursor's Continuity design](https://cursor.com/blog/git-at-any-scale) makes a
normal local Git repository a materialized cache over an immutable object-store
WAL and uses a CAS-updated WAL index as the publication point. Netz should
reuse these properties without assuming one shared S3 service:

- immutable push/WAL/pack data is durable before acknowledgement;
- a small reference/index update is the linearization point;
- notifications accelerate catch-up but reconciliation establishes truth;
- local repositories are materialized caches and can be rebuilt;
- compaction outputs are immutable objects published by a new reference;
- normal local formats and tools remain usable on a materialized replica.

In a peer deployment the authoritative durability boundary may be a configured
receipt quorum rather than S3. That is weaker than a highly durable object
service if all peers share a failure mode, so receipts report the actual owner
and site constraints they satisfy.

## Integration options

### A. Explicit immutable-object API (recommended first)

Expose `put`, `get`, `ensure-local`, `repair`, and `delete` operations which
take a connected Konserve store plus an explicit object descriptor and policy.
The result distinguishes local commit, remote quorum and later repair.

Advantages: honest failure semantics, compatible with binary and EDN values,
portable across backends, and easy to demonstrate. Applications choose exactly
which values are immutable and when a root may publish.

Cost: applications call the adapter at their publication boundary. Existing
write sites need a small integration change.

### B. Konserve write hook/outbox

Observe successful writes marked `:immutable? true`, append them to a durable
outbox, and replicate asynchronously. The hook may be useful convenience on
top of A, but it cannot make the originating call wait for quorum and a hook
receives no replayable binary stream after `bassoc` unless it reopens the value.

Use this for eventual background durability, not for a root-last transaction
which claims quorum durability.

### C. Tiered Konserve backend

Treat a local store as the frontend and a Netz object service as another tier.
Reads can fall through and repair the frontend; writes can return an explicit
backend receipt. This is attractive for pretrained-rstr and disposable local
materializations.

It should follow A after read repair, cancellation, stream ownership and
inventory semantics are tested. A peer set is not a single ordinary backend:
placement, partial failure and deletion need richer status than a boolean
write result.

### D. Transparent PStore wrapper

Intercept every Konserve protocol operation and emulate a distributed store.
This is not recommended for the first release. Nested update functions are not
portable, mutable operations need conflict semantics, atomic multi-key batches
need a publication protocol, and one-shot streams cannot be replayed to several
destinations without staging.

### E. Physical backend mirroring

Copy a Konserve backend's private files or blobs. This can be an efficient
operator-specific backup, but it is not a portable Netz standard. Layout,
encryption headers, migrations and atomicity belong to that backend.

## Proposed adapter contract

The first adapter should be explicit and asynchronous:

```text
ObjectDescriptor {
  store_id         opaque logical store namespace
  application_key  opaque application key bytes
  media_type       optional text
  privacy_domain   bytes
  retention        policy
}

put_immutable(store, descriptor, durability) -> {
  local          committed | already_present
  object         wire object address
  manifest       transfer manifest address
  durability     quorum | pending | unavailable
  receipts       verified receipt summaries
}

ensure_local(store, application_key, object, providers) -> verified local value
```

`store_id` scopes an application mapping; it is not a secret or a backend path.
The descriptor placed on the wire must contain only language-neutral data. The
application-key mapping is signed by the catalog/root authority, or is supplied
through an already authenticated closure; possession of an object address does
not authorize writing an arbitrary local key.

## Test campaign

The next implementation should add the following simulator and black-box
tests.

1. **Publish ordering:** no root is visible before every required object has a
   qualifying durable receipt.
2. **Lost notification:** a peer misses all live announcements, observes a
   newer head, reconciles the gap and fetches only the missing closure.
3. **Concurrent heads:** single-writer rejects equivocation, fenced mode rejects
   an obsolete epoch, and multi-head mode preserves both forks.
4. **Stream bounds:** a large binary value is transferred with bounded memory,
   cancellation and exact ownership/closure of input streams.
5. **Backend matrix:** file and memory on JVM, IndexedDB/Node on JavaScript, and
   the Python object store pass the same object/receipt fixtures.
6. **Materialization:** delete a local Geschichte repository or continuation
   cache and reconstruct it from object receipts plus the authoritative head.
7. **Repair:** corrupt one local object, detect it on read/scrub, retrieve a
   verified replacement and restore the desired placement count.
8. **Privacy lifecycle:** an expired offline peer cannot serve on return until
   it reconciles leases/deletions; a tombstone prevents resurrection.
9. **Compaction:** publish a compacted immutable pack/chunk set, move the head,
   and retain or collect the old closure according to explicit policy.
10. **Scale simulation:** thousands of small repositories/forks and a few hot
    repositories share bounded queues without one slow object or topic causing
    global head-of-line blocking.

The journal-first portion of item 8 is implemented and covered on JVM and
Node: restart precedes authorization, exact retention persistence gates byte
transfer, and signed leases/deletions have a verification entry point. Actual
erasure remains backend-specific. The generic Konserve adapter refuses to
claim deletion unless an explicit callback can erase all retained
representations; shared-piece garbage collection and operational offline-peer
reconciliation remain release gates.

## Delivery order

1. Keep `netz/blob-replication/1` experimental and implement a streaming object
   source/sink over a connected Konserve store. The small-value Konserve
   repository and authenticated provider-fetch path are implemented; bounded
   streaming remains.
2. Add explicit application-key mapping and `ensure-local`; do not yet wrap
   every `PStore` operation.
3. Demonstrate Dvergr/Simmis drive replication and one Geschichte pack closure.
4. Add signed head reconciliation and materialization tests using the existing
   root-log and Spindel workspace gate.
5. [local inventory/scrub and explicit provider fetch done] Add network
   inventory exchange and repair scheduling, then expose `ensure-local` and a
   tiered Konserve convenience adapter.
6. Run independent JVM/Node/Python conformance before advertising the blob
   capability by default.
