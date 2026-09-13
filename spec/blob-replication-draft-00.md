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
Layout.address(statement) -> bytes32
Layout.reconcile(state, verified_statement) -> ReconcileResult
Layout.retention_authorized(state, local_authority, retention) -> boolean
BlobStore.put(ciphertext, durability) -> DurabilityReceipt
BlobStore.ensure_local(binding, retention, providers) -> LocalityResult
BlobStore.read_installed(binding, max_bytes) -> bytes
BlobStore.status(object_id) -> ReplicaStatus
BlobStore.inventory(cursor, limit) -> ManifestPage
BlobStore.scrub(manifest_id) -> ScrubStatus
BlobStore.repair(object_id) -> completion
BlobStore.revise(layout_envelope, retention_envelope) -> completion
BlobStore.delete(object_id, authorization) -> DeletionStatus
ApplicationStore.materialize(binding, verified_bytes) -> InstallResult
~~~

The network object is the logical byte sequence presented at this boundary,
not a Konserve backend's private serialized file. Application data is opaque
bytes and is not restricted to the control-envelope CBOR subset.

## 3. Signed layout, reconfiguration and policy

Replica authority is carried by a closed `blob/layout` statement inside the
signed Netz envelope defined in section 6. Its body is:

~~~text
{
  "v": 1, "type": "blob/layout",
  "cluster": bytes,
  "epoch": uint,
  "placement": "netz/blob-placement/2",
  "seed": bytes32,
  "previous": bytes32,
  "members": [
    {
      "authority": authority_bytes32,
      "domains": {<visible-ASCII dimension>: <visible-ASCII value>, ...},
      "enabled": boolean
    }, ...
  ],
  "issued_at": uint,
  "signer": controller_authority_bytes32
}
~~~

There are 1..32 members with unique full Authority IDs. Each member has at most
16 domain entries. Domain names and values are 1..128 bytes of visible ASCII;
this deliberately avoids language-dependent Unicode normalization and ordering
inside an authorization record. `enabled` is mandatory. Member order carries no
placement priority, but the signed array MUST be sorted by unsigned Authority
ID bytes so the same membership set has one canonical representation.

The layout address is:

~~~text
layout_id = SHA-256(UTF8("netz/blob-layout/v1\0") ||
                   deterministic-CBOR(layout_statement))
~~~

A deployment configures the exact cluster and controller Authority IDs outside
peer discovery. After verifying the statement envelope, a replica reconciles
the body as follows:

1. The first accepted statement MUST have epoch zero and an all-zero
   `previous` address.
2. A successor MUST increment the accepted epoch by exactly one and its
   `previous` value MUST equal the accepted `layout_id`.
3. The same statement address at the current epoch is an idempotent duplicate.
   Lower epochs are stale; skipped epochs and bad predecessor links fail closed.
4. A different valid address at the current epoch is controller equivocation.
   The replica MUST retain bounded fork evidence and MUST stop layout-based
   retention admission and further reconfiguration permanently for that
   cluster identity. Section 3.1 defines recovery by cluster rollover.

Draft-00 signatures are single-Authority statements, not multisignatures. A
deployment that permits several controller Authorities MUST externally fence
them, for example with a single Urd/Raft leader. Learning an Authority through
membership, pub/sub, or provider discovery never makes it a layout controller
or replica. The accepted chain MUST cross the deployment's durable boundary
before its new epoch authorizes retention; the reference pure reconciler does
not itself provide that storage boundary. The reference runtime stores one
bounded ordered envelope transcript per cluster, re-verifies every signature
and predecessor before readiness, atomically persists the enlarged transcript,
and only then exposes the new epoch. A corrupt, oversized, gapped or otherwise
unreplayable durable transcript fails startup closed. Draft-00 bounds the
reference transcript to 256 envelopes by default; deployments MUST configure
a finite bound and arrange an audited checkpoint or migration before reaching
it.

### 3.1. Controller-fork recovery

Two valid, differently addressed layout statements for the same configured
cluster and epoch, each signed by an authorized controller, prove controller
equivocation. In draft-00 that proof is terminal for the cluster identity. An
implementation MUST NOT choose a branch by arrival order, wall-clock time or
LWW, and an operator MUST NOT erase the evidence or unfreeze that cluster.
Recovery creates a new cluster rather than trying to make independently
observed peers agree on an unsigned branch choice.

The operator rollover procedure is:

1. Stop new layout-based retention admission for the old cluster and preserve
   its complete durable transcript and all observed fork envelopes for audit.
2. Audit the controller and deployment configuration, and inventory every
   reachable member's retained objects, live leases, deletion tombstones and
   outstanding repair obligations. A fork is not permission to delete or serve
   bytes outside the already accepted lifecycle policy.
3. Generate a fresh unpredictable cluster identifier and provision the exact
   controller Authority out of band. A fresh controller Authority SHOULD be
   used unless compromise has been excluded. Its first layout MUST be epoch
   zero with an all-zero predecessor.
4. Explicitly authorize the new cluster and controller on each intended member,
   then durably accept its genesis layout before advertising readiness for
   that cluster or accepting its retention work.
5. Issue new retention statements under the new cluster and layout. Object and
   manifest addresses are cluster-independent, so a replica MAY reuse verified
   local bytes, but old-cluster receipts MUST NOT count toward new placement or
   quorum. Each selected target must scrub or install the object and issue new
   evidence under the new retention statement.
6. Preserve the old transcript and continue enforcing its accepted expiry,
   deletion and tombstone state until the deployment's retention and erasure
   obligations are fulfilled. The new cluster does not supersede that state.

Several allowed controller Authorities still require one external writer
fence, such as an Urd/Raft leader. Key rotation that retains the same Authority
ID does not recover a fork; the fresh cluster identity is the convergence
boundary visible to every implementation.

Placement converts each member Authority to its lowercase 64-hex spelling and
uses the following policy:

The policy contains:

~~~text
{
  "replicas": N,
  "write_acks": W,
  "placement_minimum_domains": {<dimension>: <distinct values in N>, ...},
  "ack_minimum_domains": {<dimension>: <distinct values in W>, ...}
}
~~~

Draft-00 permits `1 <= W <= N <= 5`. Each placement domain minimum MUST be no
greater than `N`; each acknowledgement domain minimum MUST be no greater than
`W`. Missing domain declarations do not count as distinct values. The selected
N-set MUST satisfy the placement minima and contain at least one exactly-W
subset satisfying all acknowledgement minima. An implementation MUST fail
closed rather than silently weaken either requirement or make W misleading.

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
  "seed": lowercase_hex(layout.seed),
  "member": member.id,
  "object": object_id,
  "profile": "netz/blob-placement/2"
}))
~~~

The signed seed MUST be exactly 32 bytes; its lowercase hex
representation is used only in the score input. Missing seeds and unknown
placement profiles MUST be rejected. Ordinary successor layouts MUST preserve
the seed. Epoch fences authority and receipts but does not change placement.
Explicit seed rotation is not supported by this initial format.

`object_id` is an opaque byte string or text identifier fixed by the negotiated
address profile. Scores sort by unsigned byte value, descending; equal scores
sort by member ID, ascending. Select the first `N` ranked members if they meet
every domain constraint. Otherwise enumerate rank-preserving `N`-combinations
and choose the first satisfying set. Search is capped at 250,000 candidates.

`fixtures/blob-layout-1.json` pins the closed statement, deterministic CBOR,
layout address and Ed25519 signature across JVM, JavaScript and Python. The
placement corpus is `fixtures/blob-placement-1.json`; it pins the
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

### 5.1. Application-object binding

An authenticated application catalog or root MAY bind its own immutable key to
one Netz object without making that key part of the transfer manifest:

~~~text
{
  "v": 1,
  "type": "blob/application-object",
  "store_id": <visible ASCII text, 1..256 bytes>,
  "application_key": <opaque bytes, 1..4096 bytes>,
  "codec": <visible ASCII text, 1..128 bytes>,
  "object": <32-byte object address>,
  "manifest": <32-byte manifest address>
}
~~~

This is a closed deterministic-CBOR object. Its optional catalog address is:

~~~text
binding_id = SHA-256(UTF8("netz/application-object/v1\0") ||
                        deterministic-CBOR(binding))
~~~

`store_id` is a logical application namespace, not a backend path or secret.
`application_key` is deliberately opaque: a drive can use UTF-8 SHA-256 hex,
while a Datahike profile can use deterministic-CBOR UUID bytes. `codec` names
the application interpretation of the verified logical bytes.
`bytes/identity` means the exact object bytes are installed unchanged; other
codec identifiers require an application profile and allowlist.

The binding MUST be carried by an authenticated and reconciled catalog/root or
otherwise authorized by equivalent application policy. Possession of a binding
or object address is not read or write authorization. Before retrieval, the
binding's object and manifest MUST exactly match the verified retention
statement. An installer MUST reject an unsupported codec and MUST NOT overwrite
an existing different value at an immutable application key. It may report
success only after reaching its claimed local persistence boundary.

Ordinary Konserve `get` remains local. Materialization is an explicit sequence:
scrub locally, fetch from a caller-authorized provider when needed, scrub the
installed Netz object, reconstruct and verify its logical bytes, then invoke the
application installer. The reference small-value API assembles at most 64 MiB
by default; larger objects require the future bounded streaming source/sink.

`fixtures/application-object-binding-1.json` pins the canonical binding and
domain-separated address across JVM, JavaScript, and Python.

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
  "placement_minimum_domains": {text: positive_uint, ...},
  "ack_minimum_domains": {text: positive_uint, ...},
  "issued_at": uint, "retain_until": uint,
  "signer": controller_authority_bytes32
}
~~~

The configured deployment policy MUST authorize the controller signer. The
current reconciled `blob/layout` MUST match the cluster and named layout epoch,
and deterministic placement MUST select the receiving replica. The retention
statement does not make a discovered peer a replica and does not replace the
durable layout chain.

A controller may extend already admitted replicas without retransmitting
placement policy using `blob/lease`, which has the common namespace, manifest,
object, layout epoch, time and signer fields plus `revision` and
`retain_until`. A lease MUST name the replica's currently retained layout
epoch; it cannot migrate placement or authorize a fresh `blob/offer` because it
does not carry `N`, `W`, or domain constraints. A coordinator that needs
continued repair or fresh receipts MUST issue a higher complete
`blob/retention` statement instead.

A higher complete retention statement MAY retain the same layout while
changing policy, or name a later accepted layout epoch. It MUST preserve the
object, manifest, privacy domain and controller stream, MUST NOT reduce the
retention deadline, and MUST have a nondecreasing issue time. The new policy is
placed against the named layout and requires newly fenced receipts from its
entire target set. A new target starts at an attempt that cannot collide with a
prior remove/re-add cycle. The complete successor intent MUST cross the durable
outbox before any new offer.

The controller requests global deletion using `blob/deletion`, replacing
`retain_until` with `delete_after`. A deletion at a later layout epoch applies
to replicas still retaining an older epoch; removed replicas otherwise retain
under their old finite deadline until expiry. New retention is admitted only
on members selected by its current reconciled layout, so dissemination alone
cannot extend a removed replica. Revisions form one monotonically increasing
controller stream per
`(cluster, privacy_domain, object)`. A replica MUST reject stale revisions and
retain evidence of same-revision conflicts. A lease may only extend the
accepted retention time. An accepted deletion is absorbing for that tuple: a
later lease cannot resurrect the same addressed object in the same privacy
domain.

A controller authorizes removal of one surplus physical replica after a
direct layout transition using `blob/retirement`:

~~~text
{
  "v": 1, "type": "blob/retirement",
  "cluster": bytes, "privacy_domain": bytes,
  "revision": positive_uint,
  "manifest": bytes32, "object": bytes32,
  "layout_epoch": uint,
  "layout": current_layout_address_bytes32,
  "previous_layout": previous_layout_address_bytes32,
  "replica": retiring_replica_authority_bytes32,
  "retention": signed_current_retention_envelope,
  "receipts": [signed_current_stored_envelope, ...],
  "issued_at": uint, "retire_after": uint,
  "signer": controller_authority_bytes32
}
~~~

`receipts` MUST contain exactly one envelope for every one of the `N` targets
selected by the embedded retention policy and current layout. It is sorted by
unsigned replica Authority bytes and contains no duplicates. Every receipt
MUST have a valid replica signature and exactly match the namespace, object,
manifest, current layout epoch, retention revision and retention deadline. Its
persistence profile MUST be explicitly accepted by the retiring replica and
MUST NOT be `netz/store/volatile/1`. All receipts and the retention statement
must remain unexpired when the command is evaluated.

The retiring replica MUST verify the controller signature, the embedded
retention signature, controller authorization and a current, non-forked layout
chain. Its configured controller set MUST exactly match the set which admitted
the layout, and the current layout signer MUST belong to that set. `layout`
MUST be the exact address of that current layout,
`previous_layout` MUST be its predecessor link, the current epoch MUST be
positive, and current deterministic placement MUST exclude `replica` while
satisfying the retention domain constraints. The command revision follows the
same controller stream and MUST be higher than the embedded retention
revision. Evaluation fails closed before `retire_after`.

A valid retirement command is evidence for considering one local deletion; it
is not a transferable delete capability and does not request global deletion.
Immediately before physical removal, the addressed replica MUST serialize a
recheck of the exact command, current authority, its durable local retention
record, pins, leases and in-progress reads or transfers with an idempotent
storage deletion. It MUST preserve the object if any protection changed or if
the command no longer verifies. Crash replay may repeat the same retirement
generation, so deletion MUST be idempotent. Draft-00 defines no network receipt
for retirement completion; the replica records completion in its local durable
migration state. `blob/deletion` and `blob/deleted` remain the distinct global
namespace-deletion mechanism.

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

The reference portable outbox representation is a local storage record, not a
network frame:

~~~text
{
  "v": 1, "type": "blob/replication-intent",
  "cluster": bytes, "object": bytes32,
  "manifest": blob_manifest,
  "layout": signed_layout_envelope,
  "retention": signed_retention_envelope,
  "intent_revision": uint,
  "attempts": [{"replica": bytes32, "attempt": uint}, ...]
}
~~~

`attempts` is sorted by unsigned replica Authority bytes, contains no
duplicates and names exactly the deterministic target set. The record itself
is not a new authority statement: on every restore an implementation MUST
re-verify both envelope signatures, configured controller authorization,
cluster, object and manifest addresses, layout epoch, placement policy and
target set. It MUST restore no more than its configured intent bound. Restored
intents release no network work until durable receipts have replayed and a
repair event has persisted its next intent revision. An implementation may use
another local representation only if it preserves those semantics.

The reference runtime serializes begin, explicit repair and receipt events for
one controller process. It loads intents before receipts and exposes readiness
only after both have completed. A failed outbox write emits no offer; retrying
the operation writes the identical pending generation. The JVM file repository
atomically replaces this record at an fsync boundary, while the portable
Konserve repository supports an explicitly accepted best-effort boundary.
When the transfer and controller runtimes share a repository, receipt replay
MUST have one owner so that transfer startup cannot race ahead of intent
restoration.

Signed intent revision is serialized on the same lane. The replacement MUST
keep the immutable manifest and monotonically advance the controller retention
stream and local intent revision. It invalidates all old receipt authority,
advances retained-target attempts, derives the new target set from the signed
policy and layout, and persists that complete record before dispatch. The
currently reconciled non-forked layout MUST equal the replacement layout at
dispatch time. An exact repeated request is idempotent; a conflicting or stale
successor fails closed.

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
Independent implementations use the portable extension carrier in Netz
draft-00 Section 11.1 and therefore require both `wire/netz-cbor/1` and
`netz/extension-cbor/1` in addition to `netz/blob-replication/1`. The named
envelope capability MUST exactly equal the blob capability; payload-shape
matching alone is not negotiation.

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

`blob/stored` has no reciprocal receipt ACK in draft-00. The receiver may close
its local session after sending it, but transport send completion does not prove
that the coordinator durably recorded it. If the connection closes first, the
controller retains missing work and a later fenced retry causes an already
installed receiver to reverify the object and reissue a fresh receipt. A client
that wants one-shot confirmation SHOULD keep the connection open until an
application/controller signal confirms persistence. A future wire revision may
add an explicit receipt ACK; it MUST be capability/version negotiated rather
than inferred from WebSocket close or write completion.

`fixtures/blob-statements-1.json` pins the retention transcript, Ed25519
signature, complete canonical offer/fetch hashes, and the need/piece/ACK/commit
transfer trace across JVM, JavaScript and Python.

The Python reference exposes one transport-independent `BlobSender` or
`BlobReceiver` per session. It returns explicit actions for carrier sends,
piece/staging IO, installation, receipt signing and receipt persistence. A
runtime owns the bounded session table and feeds completion results back
serially. This split is non-normative API structure, but preserves the same
normative boundaries: an authorized receiver starts only after lifecycle
admission, `verified_stored` is called only after signature and authenticated
remote-Authority verification, and controller release follows successful
receipt persistence.

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
missing marker is distinct from corruption or IO failure. This interface is a
local administrative and repair primitive. An implementation MUST NOT expose
its results to a peer merely because that peer negotiated
`netz/blob-replication/1`.

Draft-00 uses an authenticated targeted re-offer as its remote repair and audit
primitive. For one current retention intent, the coordinator selects an exact
target from the accepted, non-forked layout and sends that Authority an
ordinary `blob/offer` with a fresh session and attempt. The receiver MUST apply
the normal controller, layout, lifecycle and authenticated-peer authorization
before observing or revealing local storage state. Missing or corrupt pieces
are then repaired through the ordinary `blob/need` exchange. If the object is
already installed, the receiver MAY omit the need and piece exchange only
after re-verifying the marker, pieces, complete object and claimed persistence
boundary; it then signs a receipt for the new fenced attempt. The coordinator
MUST accept this response while awaiting `blob/need`, subject to the same
signature, peer, manifest, revision, epoch, attempt and persistence checks as
a receipt following `blob/commit`. It need not request retransmission of
already verified bytes merely to advance its local sender phase. The coordinator
MUST verify and durably record that exact receipt before treating the audit or
repair as successful. A receipt is point-in-time evidence, not proof that a
Byzantine peer will retain the bytes afterward.

Automatic repair scheduling MUST be driven from durable retention intents,
resume after restart, bound concurrent sessions and queued work, and use
bounded retry with backoff. The coordinator MUST durably persist each
incremented per-target attempt and its intent revision before sending that
retry; stale persistence completions and receipts from earlier attempts MUST
NOT release or satisfy the retry. Connection discovery resolves an already
selected full Authority to a live peer; it MUST NOT change the target or grant
storage authority. Network-wide inventory exchange is not defined in draft-00.
A scheduler MUST NOT issue a second concurrent repair for the same intent
generation. A result from an older layout epoch, intent revision, or target set
MUST NOT mutate the replacement generation. Implementations MAY keep cadence
and backoff timestamps in volatile memory when the durable controller intent
retains the attempt fence; after restart they MUST reconstruct incomplete work
from durable intents and current receipts and apply an initial grace interval
before issuing another generation. This scheduling repairs targets lacking
current receipts. It does not establish continuing possession by an already
acknowledged or Byzantine replica.
A later batched or Merkle extension must define cluster and privacy-domain
scoping, authorization before existence disclosure, freshness and replay
behavior, and request, response and computation bounds. It is an optimization,
not a prerequisite for correct repair.

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

A controller claiming coordinated erasure MUST durably retain the set of all
known holders, including former placement members and replicas learned from
valid storage receipts. That holder set is monotone for one deletion statement.
It MUST NOT declare the operation complete until it has durably accepted one
matching `blob/deleted` receipt from every holder. Carrier delivery, current
membership, failure-detector output, `W` storage receipts, and administrative
removal are not deletion receipts. A newly discovered holder MUST reopen an
otherwise complete campaign until its receipt arrives. Controllers SHOULD
retry the signed deletion through an authenticated targeted control stream;
pub/sub MAY additionally announce it but is not durable delivery evidence.

## 9. Required conformance work

Before advertising a durable-blob capability, implementations must additionally
complete and test:

1. operational tooling for the terminal cluster-rollover procedure in section
   3.1 (the portable signed statements, hash chain, persist-before-activate
   runtime, Konserve restoration, admission fence and normative recovery
   procedure are implemented);
2. browser durable piece staging (the Python one-session sender/receiver
   binding and fsync/atomic file repository now share JVM/JavaScript transfer
   trace bytes, JVM piece paths, marker bytes, restart/resume, inventory and
   scrub behavior; a reconnecting Python network runtime remains separate);
3. restart recovery against real durable non-memory stores beyond the JVM and
   Python file repositories (the portable lifecycle-policy adapter, JVM
   capability-gated transfer runtime, local inventory/scrub API and durable
   receipt replay are implemented);
4. operational clock-skew and real-store deletion tests (the journal-first
   pure lease/expiry/deletion machine is implemented);
5. independent JVM, JavaScript and Python end-to-end tests; and
6. real three-device tests in addition to deterministic fault simulation.
