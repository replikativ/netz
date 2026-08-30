# Netz Protocol — draft-00

Status: implementation draft, not an Internet Standard. The normative words
MUST, MUST NOT, SHOULD and MAY are used as described by RFC 2119/8174.

This document separates the prospective network protocol from the Clojure
reference implementation. The current implementation still carries native
values through Kabel; wire profile 1 is the convergence target for independent
implementations and is not advertised as interoperable until the conformance
gate in §12 passes.

## 1. Purpose

Netz provides:

1. self-certifying transport-peer identity;
2. bounded peer discovery, explicitly separate from application authority;
3. authenticated topic dissemination;
4. bounded anti-entropy repair for recent publications;
5. content-addressed block discovery and transfer; and
6. application-defined state synchronization when message repair is
   insufficient.

Netz does not define a database, CRDT, consensus algorithm, authorization
policy, moderation system, or mandatory DHT. Those are profiles or application
policy.

## 2. Layering

~~~text
application data model and authorization
  synchronization profile (root-log, join-semilattice, ...)
    Netz dissemination, repair, membership and content exchange
      reliable ordered byte-stream transport (WebSocket binding first)
~~~

Discovery is never authority. In particular, a Raft validator set MUST come
from application configuration or an authenticated reconfiguration log; peer
exchange and content-provider records MUST NOT add voters.

## 3. Common programming interface

Language bindings SHOULD expose asynchronous equivalents of:

~~~text
Node.start(configuration) -> Node
Node.close() -> completion
Node.peer_id -> PeerId
Node.publish(topic, value) -> PublishReceipt
Node.subscribe(topic, SyncProfile, callbacks) -> Subscription
Node.provide(block_id, bytes) -> completion
Node.fetch(block_id) -> bytes
Node.fetch_tree(root_id, have_roots) -> async blocks
Node.metrics() -> Metrics

SyncProfile.describe(topic) -> opaque summary bytes
SyncProfile.respond(topic, remote_summary) -> async SyncChunk
SyncProfile.apply(topic, chunk) -> completion
SyncProfile.commit(topic, checkpoint) -> completion
~~~

`publish` completing means the local runtime accepted responsibility for the
publication. An implementation MUST NOT report success for an item dropped by
a bounded queue. If application delivery fails after network acceptance, the
node MUST mark the topic desynchronized and run the state-sync profile.

Callbacks MUST be serialized per subscription unless the profile explicitly
declares concurrent application safe.

## 4. Data model and canonical CBOR

Wire profile 1 uses RFC 8949 CBOR. Signable objects use Core Deterministic
Encoding. Maps use UTF-8 text keys. The portable value subset is:

- null, booleans;
- signed integers representable in 64 bits;
- byte strings and UTF-8 text strings;
- finite arrays; and
- maps whose keys are text strings.

Indefinite-length items, floats, duplicate map keys, sets, keywords, symbols,
ratios, records, arbitrary tags and shared-reference extensions MUST NOT occur
in a profile-1 signable object. A decoder MUST reject trailing bytes.

Transport frames need not be deterministic when unsigned, but implementations
SHOULD use the same encoder everywhere. The reference profile does so.

Every protocol message is a map containing:

~~~text
{"v": 1, "type": <text>, ...}
~~~

Unknown versions MUST be rejected. Unknown message types MUST be ignored only
after capability negotiation establishes a compatible extension; otherwise
they are malformed input.

The first fixture is published in `fixtures/profile-1.json` and exercised by
both the Clojure and dependency-free Python codecs.

## 5. Resource limits

Before authentication, a connection accepts only `hello`, `proof` and
`close`. Implementations MUST enforce limits before expensive hashing,
signature verification, allocation, or collection traversal.

Profile-1 defaults are:

| Resource | Limit |
|---|---:|
| encoded WebSocket message | 5 MiB |
| decoded nesting | 64 |
| decoded scalar/container items | 16,384 |
| one text or byte string | 1 MiB |
| announced addresses | 16 |
| peer-exchange entries | 64 |
| content nodes per batch | 256 |
| handshake messages | 4/connection |
| incomplete identity handshake | 15 seconds |

Bindings MAY negotiate lower limits. A peer MUST NOT cause its counterpart to
allocate an unbounded outbound queue: transports need byte credits or an
equivalent bounded asynchronous send contract.

## 6. Identity

A genesis object is:

~~~text
{
  "v": 1,
  "rotation": [<32-byte Ed25519 public key>, ...],
  "threshold": <positive integer>,
  "operational": [<32-byte Ed25519 public key>, ...],
  "revoke": <32-byte SHA-256 commitment>
}
~~~

The 32-byte authority identifier is:

~~~text
SHA-256(UTF8("netz/peer-id/v1\u0000") || deterministic-CBOR(genesis))
~~~

Human-readable encodings are presentation only. A routing implementation MAY
project the authority identifier to a smaller key, but authorization and
signature validation MUST use all 32 bytes.

Operational private keys are hot. Rotation private keys and the revocation
preimage MUST be stored behind separate trust boundaries. Rotation-chain and
revocation messages remain RESERVED in draft-00; an implementation MUST say
that rotation is unsupported rather than accepting an unverified replacement.

## 7. Connection handshake and capabilities

Each side sends a `hello` containing its genesis object, selected operational
key, addresses, monotonically increasing record sequence, a fresh 32-byte
challenge and capabilities. The identity record is signed over deterministic
CBOR with domain `netz/identity-record/v1`.

Each side then sends `proof`, an Ed25519 signature over:

~~~text
UTF8("netz/challenge/v1\u0000") || local_challenge || remote_challenge
~~~

The connection becomes identified only after genesis derivation, record
signature and proof all verify. A second `hello` or `proof` after completion is
a protocol error. Failed or timed-out handshakes close the connection.

Capabilities are text identifiers such as `dissemination/1`, `content/1`,
`sync/1` and `transport/websocket-cbor/1`. Optional features MUST NOT be sent
until both peers advertised them.

## 8. Dissemination and recent-message repair

A signed publication contains:

~~~text
{
  "v": 1, "type": "gossip", "topic": <text>,
  "origin": <32 bytes>, "epoch": <uint64>, "seq": <uint64>,
  "payload": <portable value>
}
~~~

Its Ed25519 signature covers the deterministic encoding of that map prefixed
with `netz/gossip/v1\0`. Forwarding metadata (`hops`, previous transport peer)
is outside the origin signature and MUST be locally bounded.

Peers deduplicate by `(origin, epoch, seq)`. They periodically exchange `have`
summaries containing bounded interval sets and repair-store horizons. `want`
requests name bounded missing ranges. If a missing sequence is below a peer's
horizon, the subscriber enters `desynchronized` and opens a state-sync session.

**Open ordering issue:** `seq` is currently global per origin while forwarding
is selective by topic. It can identify duplicates and repair gaps, but cannot
define per-topic FIFO without waiting for sequence numbers belonging to topics
the subscriber does not receive. Draft-00 therefore does not promise source-order
delivery for dissemination. A later draft must either introduce stream-scoped
sequence numbers and synchronization frontiers or require every live profile to
be order-independent.

The subscriber state machine is:

~~~text
joining -> synchronized -> desynchronized -> synchronizing -> synchronized
                 ^                                  |
                 +------------- abort --------------+
~~~

Live deltas received while joining or synchronizing MUST be buffered in arrival
order. This does not resolve the open source-order issue above.
Overflow MUST restart state synchronization; it MUST NOT be treated as a gap
that recent-message repair can necessarily observe.

## 9. State synchronization

Synchronization is point-to-point, acknowledged and backpressured. It is not
flooded through the dissemination mesh.

Either peer may open a session:

~~~text
sync/open  {session: bytes16, topic: text, profile: text, summary: bytes}
sync/chunk {session: bytes16, index: uint64, data: bytes}
sync/ack   {session: bytes16, through: uint64}
sync/done  {session: bytes16, checkpoint: bytes}
sync/abort {session: bytes16, code: text}
~~~

The `summary`, `data` and `checkpoint` fields are opaque to Netz and defined by
the named profile. A receiver MUST apply chunks in index order and acknowledge
bounded batches. `done` becomes visible atomically through `SyncProfile.commit`;
failure before commit leaves the previous checkpoint visible.

Two initial profiles are planned:

- `netz/root-log/1`: a single fenced writer publishes signed monotonically
  increasing roots over a content DAG. A summary carries writer id, Raft term
  or fencing token, log index and root. This is the Datahike/Urd shape.
- `netz/join-semilattice/1`: peers exchange a causal frontier followed by
  missing deltas or a snapshot; apply is commutative, associative and
  idempotent. This is the Yggdrasil/Spindel shape.

Raft messages themselves require an addressed, reliable request/response
binding among a configured committee. Gossip discovery may locate committee
members but MUST NOT select them.

## 10. Content exchange

Content identifiers in the standards profile are multicodec CIDs. The initial
required hash is SHA-256. Blocks are opaque byte strings and MUST hash to the
requested CID before storage or forwarding.

`content/find`, `content/found`, `content/want`, `content/block`,
`content/dont-have`, `content/want-tree`, `content/tree-batch` and
`content/tree-done` retain the reference implementation's semantics. Requests,
batches, retries and provider records are bounded. A tree request MUST have a
deadline and MUST retry another provider after timeout or disconnect.

Two-hop provider discovery is the draft-00 baseline. A DHT is an optional
routing provider, not part of the core protocol.

## 11. Transport binding

The first binding is one CBOR message per binary WebSocket message. Both sides
MUST cap the WebSocket message before CBOR decoding. WebSocket supplies reliable
ordered delivery for handshake and sync sessions; dissemination remains
deduplicated because peers may receive the same signed publication over
multiple connections.

Dial policies MUST restrict schemes and embedded credentials. Public services
SHOULD reject loopback, link-local and private destinations learned from
untrusted peer exchange, and MUST defend against DNS rebinding and redirects at
the actual socket layer. Configured LAN seeds may use a separate explicit
policy.

Future QUIC, WebTransport or libp2p bindings MUST preserve the same identified
connection, bounded-send and ordered-sync semantics.

## 12. Conformance and stability gate

Wire profile 1 is ready to leave draft status only when:

1. every signable object has canonical-byte and signature fixtures;
2. malformed, oversized and deeply nested corpora are shared;
3. JVM, browser and at least one non-Clojure implementation agree on all
   fixtures;
4. two independent implementations complete identity, publication, repair,
   content and sync sessions;
5. a three-or-more-peer partition/rejoin test converges both root-log and
   join-semilattice profiles; and
6. version negotiation and downgrade behavior are tested.

Draft-00 reserves no permanent code points. Incompatible changes increment the
profile version. Compatible additions require capability negotiation.

## 13. Intended stack mapping

- Kabel: current JVM/JS transport implementation.
- Netz: identity, membership, dissemination, repair, content and sync sessions.
- Urd: configured consensus and fenced-writer selection.
- Datahike/Konserve: content-addressed storage and single-writer roots.
- Yggdrasil/Spindel: multiwriter CRDT state and deltas.
- Dvergr/Simmis: application programming model and orchestration.

This mapping is informative. The protocol interfaces above are the portable
contract; none of those libraries is required by an independent implementation.
