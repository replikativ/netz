# Resilient blob-store profile

Status: experimental implementation draft. Portable placement, manifests,
signed lifecycle statements, bounded sender/receiver transfer machines, a
crash-safe lifecycle journal, a JVM Kabel/Noise runtime and JVM/Python durable
filesystem repositories exist.
The capability remains explicit opt-in and is not advertised by default. This
is not yet a durability or data-protection claim of the Netz release.

## Scope

The profile makes immutable blobs held by an ordinary local Konserve store
durable across a configured set of Netz peers. It is deliberately narrower than
distributed Konserve synchronization:

- `konserve-sync` replicates logical keys, mutable database heads and selected
  reachable state;
- this profile places immutable opaque blobs, obtains durable receipts and
  repairs missing or corrupt replicas;
- Urd, a CRDT profile or an explicitly lossy register decides how a mutable
  name advances.

The optional `konserve-netz` adapter can combine a local file store with this
profile. Konserve and Netz core must not depend on one another: the adapter
depends on both and delegates non-immutable operations to the local store.

The current alpha seam is the explicit asynchronous
`netz.store.konserve-replication/assoc-immutable!`. It marks the local value
immutable, awaits the local Konserve commit, and only then awaits a supplied
Netz quorum operation. It is deliberately not yet a transparent `PStore`
wrapper: nested updates, binary stream replay, multi-key root-last ordering and
read repair need explicit semantics before ordinary Konserve calls acquire
network effects.

This is a useful experiment for three data-centre nodes, independently operated
party replicas, and intermittently connected personal devices. A Raspberry Pi
at home is especially interesting as a private custodian: several devices under
independent physical and administrative control avoid both one cloud-account
failure and one operator seeing plaintext. It is not intended to replace Ceph
or claim arbitrary distributed-transaction semantics.

## Terms

- **Object**: immutable opaque bytes identified by a cryptographic address.
- **Replica**: one independently identified storage node that has durably
  installed and verified an object.
- **Failure domain**: an operator-declared provider, region, host or party.
- **Target replication** (`N`): desired eligible replica count.
- **Write acknowledgements** (`W`): distinct durable replica receipts required
  before a write reports the requested durability.
- **Complete**: the manifest and every chunk are durably installed and verified.
- **Retention lease**: the bounded interval for which a replica is authorized
  and obligated to retain an object.

## Non-goals for profile 1

- mutable-key conflict resolution;
- distributed deletion without retained deletion state;
- erasure coding;
- open-network or anonymous storage;
- Byzantine availability;
- a DHT;
- copying a backend's private physical file format.

## Store composition

```text
application / Datahike
        |
        v
konserve-netz adapter
        |-- mutable values --> local Konserve / konserve-sync
        `-- immutable blobs -> local Konserve + Netz replica controller
                                                   |
                                     placement, receipts, repair
```

The adapter uses two stores. The application store holds application values.
A separate control store holds staging objects, the durable outbox, receipts,
layout epochs, retention leases and repair work. Keeping them separate prevents
replication metadata from recursively triggering replication.

Konserve write hooks are sufficient for best-effort asynchronous mirroring, but
not for quorum durability: hooks run after local success and their errors do not
fail the write. A store promising `W` durable copies must therefore implement
the Konserve protocols as a wrapper and await replica receipts itself.

## Object and manifest

The wire object is the logical byte sequence supplied to the adapter, not the
local backend's serialized or encrypted file. A receiver verifies the logical
bytes and may encode or encrypt them differently at rest.

Large values are a transfer manifest plus independently addressed pieces. The
closed profile-1 transfer manifest is:

```clojure
{"v" 1
 "type" "blob/manifest"
 "object" <32-byte address>
 "length" <non-negative safe integer>
 "piece_size" <positive integer, at most 1 MiB>
 "pieces" [<32-byte piece address> ...]}
```

Cluster, privacy domain, retention deadline, placement policy and controller
Authority belong to the separately signed `blob/retention` statement. Keeping
them out of the transfer manifest means identical ciphertext has one immutable
address while leases and layouts can advance without rechunking it.

The standard profile uses a language-neutral cryptographic address. A
`:hasch-v1` compatibility profile may address existing Clojure stack values,
but must not be presented as the portable standard.

## Write protocol

1. Stream the input into staging while computing chunk and object addresses.
2. Verify any caller-supplied content address.
3. Persist a replication intent before making success externally visible.
4. Atomically install the local object, or verify that an identical object is
   already installed.
5. Select targets from the signed layout epoch and failure-domain policy.
6. Offer the manifest; transfer chunks the target does not already hold.
7. The target verifies and durably installs the complete object.
8. The target emits a receipt only after durable installation completes.
9. Verify and durably record the signed receipt locally.
10. Return when the requested `W` eligible recorded receipts exist.
11. Retain repair work until the target `N` copies exist.

The profile-1 sender keeps one piece in flight per target. It reads a piece
only when the receiver asks for its index, sends no next piece until durable
staging advances the receiver's cumulative ACK, and does not keep object bytes
in its pure control state. A timeout or disconnect releases the bounded
session and reports the exact object, replica, layout epoch and attempt to the
controller. Policy-driven retry uses a fresh session and incremented attempt;
an old receipt cannot complete it.

A timeout after local installation must report both facts. It must never turn a
local success into a false quorum success:

```clojure
{:error :netz.replication/quorum-unavailable
 :local-committed? true
 :durable-replicas 1
 :required 2}
```

Immutable retries are idempotent. A crash at any boundary leaves either an
unacknowledged staging object or a resumable durable intent.

### Reference storage boundary

`netz.replication.storage/PBlobTransferStore` is the portable asynchronous
contract behind the sender and receiver actions. Its stage action carries the
complete manifest as well as the expected index and piece address, allowing an
independent backend to re-verify bytes without reaching into machine state.
The shared action adapter normalizes successful and failed completions back
into pure-machine events.

The JVM reference implementation, `netz.replication.file-store`, uses this
layout under an exclusively owned root:

```text
pieces/<hash-prefix>/<hash-rest>.piece
manifests/<hash-prefix>/<hash-rest>.cbor
receipts/<record-hash-prefix>/<record-hash-rest>.cbor
```

Pieces are immutable and content addressed. Installation streams and verifies
every piece and the full object, then atomically publishes a closed CBOR
manifest marker. In `netz/store/fsync/1` mode it forces piece files, atomic
markers, and their containing directories; unsupported atomic moves fail
instead of silently downgrading the receipt. Receipt records are independently
content addressed, bounded during restart scanning, and their Ed25519
signatures are re-verified before replay to the quorum controller.

The dependency-free Python `netz_protocol.FileBlobStore` implements the same
piece and manifest paths, closed marker schema, deterministic CBOR bytes,
fsync/best-effort profiles, restart/resume, bounded inventory, scrubbing and
bounded object reads through asynchronous methods. Both implementations pin
the same `blob-file-store-marker-1.hex` fixture. Python does not yet implement
the transfer machine, receipt journal or Kabel/Noise blob runtime, so this is a
cross-language storage boundary rather than an interoperability claim.

Session cleanup deliberately does not delete addressed pieces. This is what
allows a new attempt to resume after a process crash or lost ACK, and it avoids
deleting a piece shared by several manifests. Lease-aware orphan collection
remains a separate gate; until it lands the reference adapter can accumulate
uninstalled staged pieces. Local paginated
installed-manifest inventory and full-object scrubbing now exist for the JVM
file, Python file and portable Konserve adapters. The JVM adapter remains the
only executable end-to-end transfer reference; Node/browser durable storage
and the Python transfer binding remain open.

### Authenticated runtime binding

`netz.replication.runtime` attaches the sender and receiver machines to the
same per-connection Kabel middleware path as the overlay. The generic extension
dispatcher contributes only authenticated connection facts, capability
checking, the existing decoded-value/rate limits, ordered delivery and
disconnect notification. Blob policy and storage do not become Kabel concerns.

Both endpoints must explicitly add `netz/blob-replication/1` to their Noise
supported-capability set before connecting. Installing the runtime alone does
not advertise it, and `replicate!` refuses before creating a sender session when
the current connection did not negotiate the exact capability.

The receiver must supply `:authorize-offer!`. This asynchronous callback sees
the verified retention statement, its frame, routing peer and authenticated
full Authority ID. It may yield true only after checking the controller and
layout against deployment policy and durably accepting the lifecycle record.
A valid signature is evidence of who signed a statement, not authority to store
it; the default callback therefore rejects every offer. Invalid signatures and
malformed extension frames close the connection.

`netz.replication.layout` provides the portable signed-layout body, address and
pure hash-chain reconciliation used by that policy. Its output is trusted only
after the ordinary `blob/signed` envelope verifies. A same-epoch fork freezes
layout-based admission; it is not resolved by arrival order or LWW.
`netz.replication.layout-runtime` is the serialized storage boundary: it
re-verifies the complete bounded envelope transcript before readiness and
persists an accepted successor or fork before exposing the resulting state.

Storage operations preserve machine backpressure. Each piece is ACKed only
after the store's stage channel succeeds, a stored receipt is signed only after
complete installation at the requested persistence level, and the sender's
controller callback runs only after the verified receipt has been durably
recorded. A bounded single event loop serializes inbound protocol decisions;
the pure machines retain their own session and timeout bounds.

`netz.replication.lifecycle-runtime` is the production adapter for that
authorization boundary. It restores its bounded journal before it consumes
offers, serializes controller revisions, waits for the exact journal write,
and then answers the transfer runtime. The portable Konserve repository
implements both the transfer and lifecycle storage contracts:

```clojure
(let [store  (konserve-store/new-konserve-blob-store
              connected-konserve
              {:persistence "netz/store/best-effort/1"})
      layouts (layout-runtime/start!
               {:store store
                :cluster cluster-id
                :controllers [controller-authority]})
      policy (lifecycle-runtime/start!
              {:identity peer-identity
               :store store
               :controllers [controller-authority]
               :layout-state-fn #(layout-runtime/state layouts)
               :lifecycle-opts
               {:persistence "netz/store/best-effort/1"
                :accepted-persistence
                #{"netz/store/best-effort/1"}}})]
  (async/<! (layout-runtime/ready layouts))
  (async/<! (lifecycle-runtime/ready policy))
  (replication-runtime/start!
   overlay-node
   {:store store
    :authorize-offer! (lifecycle-runtime/offer-authorizer policy)}))
```

Signed lease and deletion envelopes arrive through an authenticated,
reconcilable control stream and are handed to
`lifecycle-runtime/apply-envelope!`. Pub/sub may announce them, but a peer that
was offline must reconcile the authoritative stream before serving retained
objects again.

The generic Konserve adapter cannot infer physical erasure from `dissoc`:
pieces may be shared and encrypted backends may have their own reclamation
rules. Its deletion path therefore fails closed unless the caller supplies an
explicit `:delete-object!` implementation which erases every retained copy and
returns the achieved persistence boundary. A missing eraser leaves a durable
tombstone and produces no false deletion receipt.

## Read and repair protocol

Reads prefer the local store. A local miss or integrity failure causes the
adapter to contact the deterministic target set and then other known providers.
The first verified complete value is installed locally and returned. Missing or
corrupt target replicas are scheduled for repair.

Background repair compares each durable retention intent with its current,
fenced receipts. It re-offers that exact intent to a missing target selected by
the accepted signed layout. This avoids making peer-wide object enumeration a
dependency of correct repair. Scrubbing re-reads local bytes and verifies their
address.

`netz.replication.inventory` implements the local half of that contract. Pages
contain sorted, distinct 32-byte manifest addresses and use an exclusive
lowercase-hex cursor; both the page size and the underlying marker scan are
bounded. An inventory entry means only that an installation marker exists.
`scrub-object!` separately revalidates the marker, every piece and the complete
domain-separated object address. Missing objects are explicit; corruption and
IO failure never count as replicas. The capability-gated runtime now implements
explicit authenticated provider retrieval: `fetch!` sends the already
reconciled signed retention envelope to one selected peer, the provider applies
a separate read policy, loads only an installed manifest from its
Konserve/file repository, and reuses the ordinary
offer/piece/install/receipt path. A pending request accepts a response only
from that exact peer and Authority with the exact session, attempt and
retention envelope.

The same authenticated `blob/offer` is the draft-00 remote repair and audit
primitive. Offer authorization happens before the receiver checks storage. A
missing or corrupt installation requests pieces normally. An already-installed
object is re-verified by the durable store and produces a receipt for the fresh
attempt without retransmitting its pieces. Only the exact verified receipt,
after durable coordinator persistence, refreshes evidence. The resulting
evidence is still point-in-time: periodic retries cannot make an authenticated
Byzantine replica honest.

The inventory API is deliberately local and administrative. Sending a
store-wide list would disclose private manifest membership across tenants and
clusters, while the current marker does not carry enough authority or privacy
scope to make such disclosure safe. A later authorized batched or Merkle
protocol can reduce audit traffic, but must define cluster/privacy scoping,
freshness and replay rules, and resource bounds. It is not a release blocker.
The durable, bounded scheduler that restores intents and issues targeted
re-offers remains to be implemented.

This keeps Konserve as the common storage abstraction without making ordinary
Konserve reads remote. An `ensure-local` or later tiered adapter can try local
Konserve first and explicitly invoke `fetch!`; it must surface unavailable,
denied and verified-local outcomes instead of hiding them behind a plain map
lookup.

That first explicit adapter now exists as
`netz.replication.read-repair/ensure-local!`. It scrubs the local file or
Konserve blob repository, tries at most 32 caller-selected authenticated
providers sequentially through the result-bearing fetch API, and performs a
complete post-install scrub before returning `:fetched`. It reports
`:local`, `:fetched`, or `:unavailable` with bounded per-provider failures.
Provider discovery and eligibility remain inputs from the reconciled signed
layout; this helper does not turn possession of an object address into read
authorization.

The next explicit step is implemented by
`netz.replication.materialization/materialize!`. Its deterministic-CBOR binding
ties opaque application-key bytes and a codec to the exact object and manifest.
After `ensure-local!`, it reconstructs and verifies the logical bytes under a
configurable 64 MiB default bound, then calls an application installer. The
installer owns codec allowlisting, local key decoding, immutable conflict
checks and its persistence claim. The binding must be authenticated by the
catalog/root; Netz does not infer authority from content possession.
The optional Konserve binary installer implements the baseline
`bytes/identity` policy with a configured logical-store check, application key
decoder, serialized conflict detection, idempotence and post-write readback.

Current `netz.content` remains discovery and bounded transfer state. Its working
set is not a durability catalogue. The replica controller is a separate pure
state machine which may use content and chunk transfer underneath it.

## Placement

For three storage nodes, place every object on all three. At larger scale,
deterministic rendezvous placement selects `N` nodes from a signed layout. Each
node has stable identity and declared domains such as:

```clojure
{:id "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
 :domains {"owner" "alice"
           "site" "alice-home"
           "provider" "residential"
           "region" "de-home"}}
```

An acknowledgement policy can require counts across domains, not merely hosts:

```clojure
{:replicas 3
 :write-acks 2
 :minimum-domains {"owner" 2
                   "site" 2}}
```

Requiring distinct devices only protects against hardware loss. Requiring
distinct `site` values also protects against a home outage; requiring distinct
`owner` values adds an administrative/party boundary. A private three-person
deployment can use `N=3,W=2` across three homes, while a single person can use a
laptop, home Pi and a privately controlled remote device and constrain sites
without falsely claiming three independent owners. Storage-only custodians do
not need plaintext keys.

A changed layout has the next signed epoch and links the exact previous layout
address. Receipts from an old epoch remain evidence that bytes existed, but do
not by themselves satisfy placement under the current layout. A same-epoch
signed fork permanently halts new admission for that cluster identity.

Draft-00 deliberately has no branch-choice or unfreeze operation. Operators
preserve the forked transcript, audit the controller and old lifecycle state,
and roll forward to a fresh cluster identifier with an out-of-band authorized
controller and epoch-zero layout. Existing verified object bytes can be reused
because their addresses do not include the cluster, but every migrated object
needs a new retention statement and new receipts; evidence from the forked
cluster cannot satisfy the new placement. Old leases, expiry, deletion
tombstones and audit evidence remain in force until their obligations are
fulfilled. The normative procedure is in
[`blob-replication-draft-00`](../spec/blob-replication-draft-00.md#31-controller-fork-recovery).

## Failure guarantees

With `N=3`, `W=2`, and two required failure domains:

- a successful quorum result proves at least two eligible durable copies;
- the object remains recoverable after any one replica failure;
- the two-node side of a 2-1 partition can continue quorum writes;
- the singleton cannot report quorum durability;
- after repair reaches all three replicas, any two replicas may be lost while
  the object remains readable from the third;
- corruption is detected, not silently interpreted as another version.

Two failures immediately after a `W=2` receipt are not tolerated: the survivor
might be the third target which had not received the object. `W=3` removes that
window but makes every write wait for all three sites. `W=1` permits local
progress but offers no machine-failure durability before repair.

These are crash-fault and integrity-detection guarantees. Authenticated peers
can still delete data or refuse service. Durability also depends on the local
backend actually providing the persistence level named in its receipt.

## Privacy, retention and erasure

Replication makes privacy governance more important, not impossible. The
system must not equate content-addressed with permanently retained.

### Privacy domains

Plain hashes of guessable personal data leak equality and permit dictionary
testing. Private deployments therefore SHOULD use randomized authenticated
encryption before addressing, or a keyed address scoped to a privacy domain.
Provider discovery must be restricted to authorized peers. Cluster, object and
manifest metadata must be assessed independently because metadata can itself
identify people or relationships.

Encryption keys and replica authorization are separate from peer discovery.
Per-object data-encryption keys wrapped to authorized recipients give the
narrowest revocation boundary. Destroying all key wraps is useful defence in
depth, but profile 1 must support actual replica deletion and must not treat
cryptographic erasure alone as universally sufficient.

### Retention leases

Every privacy-sensitive placement has a finite retention lease. The controller
renews it while the purpose and lawful retention period continue. On expiry a
replica stops serving, deletes the object and durably records a local absorbing
tombstone. An explicit signed controller deletion additionally produces a
signed replica deletion receipt; an autonomous expiry cannot truthfully claim
a controller deletion revision that does not exist.
Long-lived public or non-personal objects may use a distinct archival policy;
indefinite retention must never be the accidental default.

An intermittently connected replica creates an unavoidable choice: it cannot
both serve indefinitely while offline and guarantee prompt receipt of an
erasure instruction. A stale replica must reconcile the control plane before
serving after its lease expires. Deployments choose a maximum offline interval
consistent with their availability and erasure obligations.

### Deletion and resurrection prevention

A signed deletion marker prevents an old replica from reintroducing an object.
It must remain until every replica has either acknowledged deletion or been
administratively retired from the trust domain. A returning node synchronizes
deletions and layout membership before it may announce or serve objects.

Profile 1 should initially expose explicit deletion and deletion receipts while
remaining conservative about automatic garbage collection. Backups, staging
files, logs and repair queues are part of the erasure inventory.

### Application responsibility

An opaque object store cannot discover which objects concern a data subject.
The application needs a subject/retention index or another effective erasure
workflow. Advancing a Datahike root to corrected state does not erase personal
data from historical commits or unreachable blobs. Datahike history and GC
policy must be composed explicitly with replica deletion.

The parties operating replicas also need declared roles and instructions. A
topology does not decide whether they are controllers, joint controllers or
processors. Placement policy should record permitted jurisdictions and parties,
but legal basis, contracts, subject rights and transfer safeguards remain a
deployment responsibility.

This section is privacy-engineering guidance, not a claim that enabling a
configuration makes a deployment legally compliant.

## Test model

The deterministic simulator gains a durable disk per node, staging/outbox state
and the following faults:

- message loss, duplication, delay and partition;
- process crash with disk retained;
- complete disk loss;
- write failure and full disk;
- object or chunk corruption;
- layout change, retirement and node replacement;
- lease expiry and a stale replica returning after deletion.

Required invariants include:

1. no durability result before `W` distinct eligible persistence completions;
2. no complete receipt for a partial manifest;
3. no receiver installs or serves bytes that fail their address check;
4. every crash boundary resumes or remains honestly unacknowledged;
5. a healed cluster repairs toward `N` when capacity exists;
6. placement satisfies configured failure domains;
7. an expired replica does not serve before reconciliation;
8. a deletion marker prevents resurrection;
9. no automatic collection deletes an object with a live retention lease.

Integration tests use three real file stores and real Netz/Kabel/Noise peers.
The demonstration uploads an encrypted chunked blob with `N=3,W=2` to three
private devices at independent sites, stops the uploader, retrieves from
another node, wipes and rebuilds a replica, tests a 2-1 partition, and shows
expiry/deletion reconciliation for an offline home node. It reports which
owner/site constraints a receipt actually satisfies rather than implying that
three process IDs represent three independent failure domains.

## Implementation order

1. [done] Freeze object, receipt, retention, deletion and signed
   layout/reconfiguration schemas with canonical CBOR fixtures. The bounded
   transcript restores through portable Konserve, and signed controller forks
   have a terminal cluster-rollover procedure.
2. [done] Implement pure placement, receipt-controller, receiver-transfer and
   crash-safe lifecycle transitions.
3. [done] Add simulated durable disks, faults, crash recovery, bulk expiry and
   resurrection-prevention invariants.
4. [done] Implement the bounded pure sender, including one-piece backpressure,
   receipt fencing and persist-before-quorum handoff.
5. [JVM and Python file stores done, portable Konserve reference added]
   Implement durable file
   staging/install, signed-receipt replay and the explicit capability-gated
   Kabel/Noise binding. The generic Konserve repository now passes JVM and Node
   memory-store restart/install tests and honestly exposes only volatile or
   best-effort completion. Python shares the JVM piece/marker format but not yet
   its receipt/runtime binding. The lifecycle-policy adapter now has JVM and
   Node Konserve restart tests; add real Node/browser durable-backend tests.
6. [targeted repair protocol chosen; scheduler open] Add durable, bounded
   background placement-repair scheduling. Authenticated targeted re-offer is
   the baseline repair/audit primitive; broad or Merkle inventory is only a
   later privacy-scoped optimization. Local-first retrieval already reuses the
   receipt-producing transfer runtime.
7. [portable binding and bounded materializer done] Add the streaming
   application-key/immutable-object adapter over the Konserve transfer
   repository; publish any tiered or transparent `konserve-netz` wrapper only
   after its stream/batch semantics are explicit.
8. Add the real JVM/Node/Python three-device conformance campaign.
9. Enable automatic reclamation only after operational deletion tests.
