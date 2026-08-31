# Netz durable blob replication — extension draft-00

Status: experimental implementation draft, not an Internet Standard and not a
claim made by the Netz alpha release. Normative words use RFC 2119/8174.

## 1. Purpose and boundary

This extension places immutable opaque blobs on an explicitly authorized set
of Netz peers and returns evidence only after durable installation. It is a
storage profile over Netz identity, authenticated transport and bounded
transfer; it is not peer discovery, consensus, mutable-key synchronization or
an open storage market.

`konserve-sync` remains responsible for logical key/database synchronization.
A future `konserve-netz` adapter may use this extension for values marked
immutable. Urd, a CRDT profile or application policy decides how a mutable root
advances. No peer learned through gossip becomes a storage replica unless it
also occurs in the verified layout.

Profile 1 assumes crash faults and detects corruption. An authenticated replica
can still delete bytes, lie about future availability, or refuse service;
Byzantine durability needs additional auditing or proofs and is out of scope.

## 2. Portable interface

Language bindings SHOULD expose asynchronous equivalents of:

~~~text
Placement.rank(layout, object_id) -> [Replica]
Placement.place(layout, policy, object_id) -> [Replica]
BlobStore.put(ciphertext, durability) -> DurabilityReceipt
BlobStore.get(object_id) -> bytes
BlobStore.status(object_id) -> ReplicaStatus
BlobStore.repair(object_id) -> completion
BlobStore.delete(object_id, authorization) -> DeletionStatus
~~~

The network object is the logical byte sequence presented at this boundary,
not a Konserve backend's private serialized file. Application data is opaque
bytes and is not restricted to the control-envelope CBOR subset.

## 3. Verified layout and policy

The placement input is a layout whose authenticity and administrative signer
have already been verified. A member has:

~~~text
{
  "id": <lowercase 64-hex full Netz Authority ID>,
  "domains": {<text dimension>: <text value>, ...},
  "enabled": <boolean, default true>
}
~~~

The layout contains `epoch`, an unsigned integer, and at most 32 members with
unique identifiers. Member array order carries no meaning. A deployment MUST
bind the policy and layout epoch in its authenticated configuration; signature
and reconfiguration authority are deployment policy in draft-00.

The policy contains:

~~~text
{
  "replicas": N,
  "write_acks": W,
  "minimum_domains": {<dimension>: <distinct values required>, ...}
}
~~~

Draft-00 permits `1 <= W <= N <= 5`. Each domain minimum MUST be no greater
than `W`, so a configured write threshold has one unambiguous meaning. Missing
domain declarations do not count as distinct values. The complete set of
constraints MUST be satisfiable in `N` slots; an implementation MUST fail
closed rather than silently weaken it.

`owner` describes an administrative/party boundary and `site` a physical
failure boundary. Three process IDs on one host are not three sites. A private
home deployment can use a laptop, Raspberry Pi and a privately controlled
remote device. Requiring distinct owners is appropriate only when those owners
really are independent; storage-only custodians need not possess plaintext
keys.

## 4. Deterministic placement

For every enabled member, compute:

~~~text
SHA-256(deterministic-CBOR({
  "epoch": layout.epoch,
  "member": member.id,
  "object": object_id,
  "profile": "netz/blob-placement/1"
}))
~~~

`object_id` is an opaque byte string or text identifier fixed by the negotiated
address profile. Scores sort by unsigned byte value, descending; equal scores
sort by member ID, ascending. Select the first `N` ranked members if they meet
every domain constraint. Otherwise enumerate rank-preserving `N`-combinations
and choose the first satisfying set. Search is capped at 250,000 candidates.

The known-answer corpus is `fixtures/blob-placement-1.json`. It pins the
canonical score input, all scores, complete ranking and selected target set for
the JVM, JavaScript and Python implementations.

## 5. Manifest and address

A portable manifest is a deterministic-CBOR control object containing at
least:

~~~text
{
  "v": 1,
  "type": "blob/manifest",
  "cluster": <opaque bytes>,
  "privacy_domain": <opaque bytes>,
  "object": <address bytes>,
  "address_profile": <negotiated text>,
  "length": <unsigned integer>,
  "chunks": [
    {"id": <address bytes>, "offset": <uint>, "length": <positive uint>}, ...
  ],
  "retention_not_after": <Unix milliseconds>,
  "created_by": <full Authority ID>,
  "signature": <bytes>
}
~~~

The portable byte-address transcripts are:

~~~text
object_id   = SHA-256(UTF8("netz/blob-object/v1\0") || object_bytes)
piece_id    = SHA-256(UTF8("netz/blob-piece/v1\0") || piece_bytes)
manifest_id = SHA-256(UTF8("netz/blob-manifest/v1\0") ||
                     deterministic-CBOR(transfer_manifest))
~~~

The closed transfer manifest has the fields `v`, `type`, `object`, `length`,
`piece_size`, and the ordered `pieces` address array. Piece size is at most 1
MiB and there are at most 16,384 pieces. `fixtures/blob-manifest-1.json` pins
the descriptor, addresses, piece data and resume bitmap across JVM, JavaScript
and Python.

Implementations MUST NOT substitute a host-language printer or hash a
backend-private representation. The higher-level retention manifest and its
signature transcript remain to be frozen before this extension can advertise
an interoperable capability.
Randomized AEAD ciphertext SHOULD be produced before content addressing when
plaintext is personal, secret or guessable; a plain hash otherwise reveals
equality and enables dictionary testing.

## 6. Transfer, attempt fencing and receipts

The coordinator persists an outbox intent before reporting success. Each store
offer binds `object`, `layout_epoch`, target replica and monotonically
increasing `attempt`. A retry increments the attempt. A completion for an older
attempt MUST NOT satisfy the current operation, including after a connection or
disk is replaced.

The receiver requests missing pieces with a bitset: bit index 0 is the most
significant bit of byte 0, and a set bit means "needed". Unused trailing bits
MUST be zero. This bounds a 16,384-piece resume summary to 2 KiB.

The receiver verifies every piece and the complete
object, atomically installs it, and flushes according to the persistence level
named in the deployment. Only then may it issue a signed storage receipt. Mere
network delivery, an accepted offer, a queued write or a partially installed
manifest is not an acknowledgement.

The replica-receipt signature transcript and resumable chunk-frame schemas are
intentionally still open in draft-00. Until they are frozen, receipts are
implementation-local and this extension MUST NOT claim cross-language durable
interoperability.

A coordinator may return quorum when it holds `W` current receipts satisfying
all domain constraints. It retains repair work until all `N` targets have
receipts. A completion timeout MUST distinguish a local commit from achieved
quorum.

## 7. Failure and privacy semantics

With `N=3,W=2`, success proves two eligible durable installations at the stated
persistence level. It tolerates loss of either acknowledged copy, but not every
possible pair of immediate failures: the third target may not yet hold bytes.
After repair reaches `N=3`, any two replicas may be lost while the third remains
readable. A 2–1 partition may reach quorum only on the two-node side; the
singleton cannot claim it.

Immutable means write-once while retained, not undeletable. Privacy-sensitive
objects require finite retention leases, restricted provider discovery, actual
replica deletion and deletion receipts. A node returning after lease expiry
MUST reconcile deletion markers before announcing or serving old bytes. Key
destruction is defence in depth and does not replace deletion from replicas,
staging, backups and repair queues.

The application remains responsible for subject/retention indexing. Moving a
Datahike root does not erase personal data from historical immutable values.

## 8. Required conformance work

Before advertising a durable-blob capability, implementations must additionally
freeze and test:

1. retention-manifest and signature transcripts;
2. offer/piece and signed replica-receipt schemas (the need bitmap is frozen);
3. bounded resumable transfers and durable outbox crash recovery;
4. lease renewal, deletion markers and deletion receipts;
5. independent JVM, JavaScript and Python end-to-end fixtures; and
6. real three-device tests in addition to deterministic fault simulation.
