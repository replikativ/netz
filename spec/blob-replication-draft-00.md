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
BlobStore.inventory(cursor, limit) -> ManifestPage
BlobStore.scrub(manifest_id) -> ScrubStatus
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

A transfer manifest is the following closed deterministic-CBOR control object:

~~~text
{
  "v": 1,
  "type": "blob/manifest",
  "object": <address bytes>,
  "length": <unsigned integer>,
  "piece_size": <positive uint, at most 1 MiB>,
  "pieces": [<32-byte piece address>, ...]
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
backend-private representation.
Randomized AEAD ciphertext SHOULD be produced before content addressing when
plaintext is personal, secret or guessable; a plain hash otherwise reveals
equality and enables dictionary testing.

## 6. Signed retention and lifecycle statements

All durable statements use the ordinary Netz operational Ed25519 key model,
not a second identity system. The exact signature transcript is:

~~~text
UTF8("netz/blob-statement/v1\0") || deterministic-CBOR(statement)
~~~

The self-contained envelope is:

~~~text
{
  "v": 1,
  "type": "blob/signed",
  "statement": <one closed statement below>,
  "genesis": <Netz profile-1 genesis object>,
  "operational": <32-byte Ed25519 public key>,
  "signature": <64-byte Ed25519 signature>
}
~~~

The Authority ID derived from `genesis` MUST equal `statement.signer`, and
`operational` MUST be authorized by that genesis. This is intentionally a
domain-separated Netz envelope rather than COSE_Sign1 in draft-00. COSE would
still require the Netz genesis-to-operational-key authorization proof and a
second CBOR sub-profile. A future COSE binding can be negotiated as a distinct
capability without changing the statements.

`cluster` and `privacy_domain` are non-empty opaque byte strings of at most 64
bytes. They prevent a valid statement from being replayed into another
administrative or privacy namespace. `manifest`, `object`, `signer`, and
`replica` are 32-byte values. Times are Unix milliseconds represented as safe
unsigned integers. Counters and epochs are also limited to the exact integer
range shared with JavaScript, `0..2^53-1`.

The initial controller statement is:

~~~text
{
  "v": 1, "type": "blob/retention",
  "cluster": bytes, "privacy_domain": bytes,
  "revision": positive_uint,
  "manifest": bytes32, "object": bytes32,
  "layout_epoch": uint,
  "replicas": N, "write_acks": W,
  "minimum_domains": {text: positive_uint, ...},
  "issued_at": uint, "retain_until": uint,
  "signer": controller_authority_bytes32
}
~~~

The configured deployment policy MUST authorize the controller signer and the
named layout epoch. The retention statement does not make a discovered peer a
replica and does not replace verification of the signed/configured layout.

A controller extends retention without retransmitting placement policy using
`blob/lease`, which has the common namespace, manifest, object, layout epoch,
time and signer fields plus `revision` and `retain_until`. It requests deletion
using `blob/deletion`, replacing `retain_until` with `delete_after`. Revisions
form one monotonically increasing controller stream per
`(cluster, privacy_domain, object)`. A replica MUST reject stale revisions and
retain evidence of same-revision conflicts. A lease may only extend the
accepted retention time. An accepted deletion is absorbing for that tuple: a
later lease cannot resurrect the same addressed object in the same privacy
domain.

A replica's successful installation statement is:

~~~text
{
  "v": 1, "type": "blob/stored",
  "cluster": bytes, "privacy_domain": bytes,
  "retention_revision": positive_uint,
  "manifest": bytes32, "object": bytes32, "layout_epoch": uint,
  "replica": replica_authority_bytes32, "attempt": uint,
  "persistence": <profile text>,
  "stored_at": uint, "retain_until": uint,
  "signer": replica_authority_bytes32
}
~~~

`replica` MUST equal `signer`. A deletion receipt has type `blob/deleted`, the
same namespace/address/layout/replica/persistence/signer fields,
`deletion_revision`, and `deleted_at`. It MUST only be issued after the object,
staging data and live repair work have been removed at the stated persistence
level. Backups outside the replica protocol remain an operator obligation.

The defined persistence profiles are:

- `netz/store/volatile/1`: process-memory or otherwise non-crash-durable;
- `netz/store/best-effort/1`: atomically installed after backend write
  completion, without a forced stable-storage barrier; and
- `netz/store/fsync/1`: content and installation metadata flushed using the
  backend's stable-storage primitive. On a POSIX file store this includes the
  file, atomic rename and containing directory metadata.

A volatile receipt MUST NOT satisfy `W`. Best-effort receipts count only when
the caller explicitly accepts that persistence profile. The durability policy
SHOULD require fsync receipts. Storage hardware or a Byzantine operator lying
about stable writes remains outside the crash-fault model.

## 7. Transfer, attempt fencing and receipts

The coordinator persists an outbox intent before reporting success. Each store
offer binds `object`, `layout_epoch`, target replica and monotonically
increasing `attempt`. A retry increments the attempt. A completion for an older
attempt MUST NOT satisfy the current operation, including after a connection or
disk is replaced.

The `netz/blob-replication/1` capability selects these transfer frames. It MUST
only be used over a connection that authenticates both full Netz Authority IDs.
An implementation MUST NOT send or accept these frames merely because a local
handler is installed: both endpoints must cryptographically negotiate the exact
capability for the current connection. The capability is intentionally absent
from the reference implementation's default set while this extension remains
experimental.
Every frame is a closed map with `v: 1` and a 16-byte random `session`:

~~~text
blob/offer  {session, attempt, replica, retention, manifest}
blob/fetch  {session, attempt, retention}
blob/need   {session, manifest, need}
blob/piece  {session, manifest, index, data}
blob/ack    {session, manifest, through}
blob/commit {session, manifest, count}
blob/stored {session, receipt}
blob/abort  {session, code}
~~~

Here `retention` and `receipt` are signed envelopes, `manifest` in an offer is
the complete transfer manifest, and other `manifest` fields are its 32-byte
address. An offer's addresses MUST match its signed retention statement and
its target `replica` MUST be the authenticated receiving Authority. A stored
frame's receipt signer MUST be the authenticated sending Authority. Structural
frame decoding alone is not signature verification.

`blob/fetch` asks the authenticated peer to serve the manifest named by the
signed retention statement. The provider MUST independently authorize the
authenticated requester and controller-signed retention statement before
revealing whether it holds the object. If authorized and installed, it answers
with an ordinary `blob/offer` using the same session, attempt and exact
retention envelope; all piece, install and receipt rules then remain unchanged.
The requester MUST accept that offer only from the selected authenticated
provider and MUST still apply its normal offer/lifecycle policy. An abort or
timeout fails the pending fetch and does not count as a replica. A signed
retention statement is durability intent, not by itself read authorization.

Before accepting an offer, the receiver MUST verify its retention signature,
authorize the controller signer and named layout under local deployment policy,
and durably accept the lifecycle statement. These are distinct checks. A valid
signature from an unknown controller MUST be rejected without staging bytes.
The transport routing peer need not itself be the controller, but its full
authenticated Authority is an input to policy and every later session frame is
bound to that same connection. Invalid signatures, non-negotiated extension
frames and extension-queue overflow fail closed by disconnecting the peer.

The receiver requests missing pieces with a bitset: bit index 0 is the most
significant bit of byte 0, and a set bit means "needed". Unused trailing bits
MUST be zero. This bounds a 16,384-piece resume summary to 2 KiB.

Pieces are at most 1 MiB and MUST match the address and expected length at
their manifest index. WebSocket delivery is ordered; `blob/ack.through` is the
highest contiguous piece index durably staged for this session. An ACK MAY be
cumulative but is only flow control, never a storage receipt. `blob/commit`
names the expected piece count and asks the receiver to verify and install the
complete object. A profile-1 sender MUST keep at most one piece awaiting an ACK
per session. It reads the next requested piece only after the cumulative ACK
covers the current index; an ACK may therefore also eliminate later requested
indices that the receiver satisfied through content-address deduplication.
Piece data MUST NOT remain in sender control state after its send action.
Sessions, staged bytes, decoded maps, in-flight pieces and outbound queues MUST
all be bounded. A reconnect or retry uses a fresh session and `blob/need` to
resume from verified pieces.

Durable staging SHOULD be addressed by manifest and piece address rather than
the volatile session ID. Session cleanup releases in-memory work but MUST NOT
delete a verified piece that is installed, shared by another manifest, or
needed for retry. Orphan collection is a separate lease-aware operation.

The receiver verifies every piece and the complete
object, atomically installs it, and flushes according to the persistence level
named in the deployment. Only then may it issue a signed storage receipt. Mere
network delivery, an accepted offer, a queued write or a partially installed
manifest is not an acknowledgement.

`fixtures/blob-statements-1.json` pins the retention transcript, Ed25519
signature and complete canonical offer/fetch hashes across JVM, JavaScript and
Python.

A coordinator may return quorum when it holds `W` current receipts satisfying
all domain constraints. Signature verification and an authenticated replica
connection are necessary but not yet a local crash boundary: the coordinator
MUST durably record each accepted signed receipt before releasing it to quorum
accounting. The receipt's object, manifest, layout epoch, replica, attempt,
retention revision/deadline and accepted persistence profile MUST exactly match
the current offer. It retains repair work until all `N` targets have receipts.
A completion timeout MUST distinguish a local commit from achieved quorum.

The local inventory interface enumerates installation-marker manifest IDs in
unsigned bytewise order, without duplicates. Profile 1 pages contain at most
1,024 IDs and use the lowercase hexadecimal form of the last returned ID as an
exclusive cursor. Presence in this inventory is not a durability receipt or an
integrity proof. `scrub` MUST separately verify the marker address, every piece
address and length, and the complete domain-separated object address. A
missing marker is distinct from corruption or IO failure. Network inventory
exchange is not defined in draft-00.

## 8. Failure and privacy semantics

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

Lease expiry depends on wall-clock time. Deployments MUST specify an allowed
clock-skew bound and monitor it. A replica whose clock is outside policy MUST
fail closed for serving and renewal rather than silently extending retention.
Deletion markers and their highest accepted revision survive object deletion;
otherwise an offline replica could resurrect bytes from a stale offer.

## 9. Required conformance work

Before advertising a durable-blob capability, implementations must additionally
freeze and test:

1. the signed/configured layout and reconfiguration transcript;
2. non-JVM durable piece-staging adapters (the bounded pure sender/receiver and
   JVM fsync/atomic-file reference adapter are implemented);
3. restart recovery against real durable non-memory stores (the portable
   lifecycle-policy adapter, JVM capability-gated transfer runtime, local
   inventory/scrub API and durable receipt replay are implemented);
4. operational clock-skew and real-store deletion tests (the journal-first
   pure lease/expiry/deletion machine is implemented);
5. independent JVM, JavaScript and Python end-to-end tests; and
6. real three-device tests in addition to deterministic fault simulation.
