# Netz signed snapshot reference profile, draft 00

Status: implementable draft. The Clojure/JVM and ClojureScript implementation
is `netz.snapshot-ref`. A second implementation and shared byte fixtures are
required before this becomes an interoperability claim.

## Purpose

A snapshot-reference statement says that one Authority published an immutable
snapshot on one named ref and names the snapshot's immediate causal parents.
It is independent of transport and storage. Receipt of a valid statement does
not imply receipt, validation, or installation of the snapshot's object
closure.

Unlike `netz/root/v1`, this profile does not impose a sequence or a single
predecessor. Statements may arrive out of order and concurrent heads remain
visible until an application-defined merge or selection produces a descendant.
This is suitable for Datahike commit DAGs, Yggdrasil systems, Geschichte refs,
and other Git-like stores.

## Signed object

The signature input is:

```text
UTF8("netz/snapshot-ref/v1\0") || deterministic_cbor(object)
```

`object` has exactly the following fields, except for optional `ordering`:

| key | type | constraint |
|---|---|---|
| `v` | unsigned integer | `1` |
| `type` | text | `snapshot-ref` |
| `profile` | text | 1..128 UTF-8 bytes |
| `system` | byte string | 1..256 bytes |
| `ref` | byte string | 1..256 bytes |
| `snapshot` | byte string | 1..256 bytes |
| `parents` | array of byte strings | 0..64 unique values, each 1..256 bytes |
| `publisher` | byte string | 32-byte Authority ID |
| `ordering` | byte string | optional, 1..512 bytes |

Parents are sorted by unsigned lexicographic byte order before encoding.
Duplicate parents are removed. CBOR uses the RFC 8949 Core Deterministic
Encoding requirements and the restricted Netz control data model.

The optional ordering value has no baseline comparison semantics. A negotiated
application profile may define it as a signed fence, such as an Urd term and log
index. Receivers that do not implement that profile treat it as opaque data;
they must not invent a last-write-wins comparison.

## Self-verifying envelope

The transmitted map contains the signed-object fields plus:

| key | type | meaning |
|---|---|---|
| `genesis` | map | Netz Authority genesis record |
| `operational` | byte string | 32-byte operational public key |
| `signature` | byte string | 64-byte Ed25519 signature |

The receiver derives the Authority ID from `genesis`, checks that it equals
`publisher`, checks that `operational` is authorized by the genesis, and then
verifies the signature. Deployments should pin the expected publisher for a
stream when it is known out of band. Trust on first use is only a bootstrap
policy.

## Stream and DAG processing

A stream is `(profile, system, ref)`. Within a stream, `snapshot` identifies
one immutable DAG node and `parents` are its immediate predecessors.

- Duplicate delivery of the same semantic node is idempotent.
- The same publisher signing two different parent/order definitions for the
  same snapshot is retained as conflict evidence.
- A child received before its parent remains a head after the parent arrives.
- A node removes known ancestor heads and becomes a head unless a known head is
  already its descendant.
- Concurrent descendants remain multiple heads. Arrival order never selects a
  winner.
- A statement that would close a causal cycle is rejected.
- Missing parent IDs are reconciliation requests, not malformed statements.

Implementations must bound streams, retained nodes, conflicts, identifiers and
parent fan-out. Reaching a local retention bound is a visible capacity failure;
it must not silently evict a node and reinterpret the remaining graph.

These rules track authenticated causality only. An application exposes a
snapshot as locally usable after its profile has fetched, verified, and
atomically installed the required immutable closure.

## Kabel binding

The snapshot/live synchronization strategy is scoped to exactly one stream and
pins its expected publisher when configured. Its client summary is a bounded,
sorted array of retained head IDs encoded as lowercase hexadecimal text. The
responder walks its own heads toward those stop points and sends each missing
self-verifying envelope as one handshake item. The live publication value is
the same encoded envelope. Both paths perform identical signature, stream, DAG,
cycle, and capacity checks.

The summary is a reconciliation accelerator, not authority: an unknown remote
head suppresses no local history. A bounded handshake that cannot carry the
required ancestry fails rather than claiming synchronization; a later profile
can replace that case with an object-backed checkpoint.

## Datahike profile

The initial `datahike/commit-v1` binding uses:

- canonical Datahike store identity as UTF-8 `system`;
- branch name without the leading Clojure keyword colon as UTF-8 `ref`;
- lowercase canonical UUID text as UTF-8 `snapshot` and `parents`;
- one event emitted after each durable grouped writer commit.

The event-to-reference bridge rejects non-durable or incomplete events. Netz
then publishes the encoded signed statement as an opaque dissemination payload.
On receipt, the immutable object closure is fetched through the explicit Netz
object/materialization boundary; the announcement itself never mutates a
Datahike connection.

Urd is not required by this binding. A later fenced single-writer profile may
define the exact bytes and comparison rule for `ordering`. Without such a
profile, multiple valid Datahike heads are surfaced rather than silently
resolved.
