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
SyncProfile.commit(topic, checkpoint, claimed_frontiers) -> CommitReceipt
~~~

`publish` completing means the local runtime accepted responsibility for the
publication. An implementation MUST NOT report success for an item dropped by
a bounded queue. If application delivery fails after network acceptance, the
node MUST mark the topic desynchronized and run the state-sync profile.

Callbacks MUST be serialized per subscription unless the profile explicitly
declares concurrent application safe.

`CommitReceipt` contains the locally committed application result, the bounded
stream frontiers that result actually represents, and their encoded byte size.
`claimed_frontiers` came from the peer and is untrusted. A binding MUST return
no frontier unless its profile can validate that the checkpoint causally or
cryptographically covers it; copying the claim into the receipt is invalid.

## 4. Control data model, payloads, and deterministic CBOR

Wire profile 1 uses RFC 8949 CBOR. Signable control objects use Core
Deterministic Encoding. Maps use UTF-8 text keys. The control value subset is:

- null, booleans;
- signed integers representable in 64 bits;
- byte strings and UTF-8 text strings;
- finite arrays; and
- maps whose keys are text strings.

Indefinite-length items, floats, duplicate map keys, sets, keywords, symbols,
ratios, records, semantic tags and shared-reference extensions MUST NOT occur
in a profile-1 control object. A decoder MUST reject trailing bytes.

This is a protocol-envelope rule, not a restriction on CBOR application data.
Profile summaries, chunks, checkpoints, publications, and content blocks carry
application encodings as byte strings. Their named application profile defines
the enclosed bytes and MAY use registered CBOR tags, including identifiers,
generic objects, sets, string references, CIDs, UUIDs, and typed arrays. A
sender signs the byte string as received; intermediaries MUST NOT decode and
re-encode an opaque payload before forwarding or verification.

Boring's shaped-array tag `39649` is provisional until it has an IANA
assignment. A profile using it MUST negotiate that capability explicitly and
MUST NOT label the payload as a baseline interoperability profile. This does
not affect Boring's registered tags or ordinary CBOR values.

Every profile-1 control frame MUST use the same Core Deterministic Encoding,
whether or not that particular envelope is signed. This keeps one closed
decoder and one conformance corpus across transports; application payloads
remain opaque and are not subject to this rule.

Every protocol message is a map containing:

~~~text
{"v": 1, "type": <text>, ...}
~~~

Unknown versions MUST be rejected. Unknown message types MUST be ignored only
after capability negotiation establishes a compatible extension; otherwise
they are malformed input.

The first control fixture is published in `fixtures/profile-1.json` and
exercised by both the Clojure and dependency-free Python codecs.
`fixtures/kabel-cbor-1.json` pins the same item's four-byte serializer-14
carrier prefix. That prefix is not part of the decoded envelope or any
signature transcript. `fixtures/malformed-profile-1.txt` is the shared negative
corpus: every listed byte string MUST be rejected by a profile-1 decoder.

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

Signed root-log profiles use the independent `netz/root-record/v1\0`
transcript pinned by `fixtures/root-record-1.json`. The publisher field is the
full Authority ID, and the record carries the genesis, operational key, and
Ed25519 signature needed for verification without prior contact. Root and
predecessor identifiers are opaque byte strings.

Capability identifiers in signed or handshake data MUST contain 1–128 visible
ASCII octets (`0x21`–`0x7e`). Signed addresses MUST be sorted by ASCII byte
order and be absolute, credential-free `ws` or `wss` URIs containing only
visible ASCII, with no fragment. Producers SHOULD use lowercase IDNA A-labels,
compressed IPv6, `/` for an empty path, and uppercase UTF-8 percent escapes.
The exact URI spelling is signed; a verifier validates but does not normalize
it, avoiding runtime-dependent URL rewriting at the trust boundary.

## 7. Connection handshake and capabilities

The direct Kabel binding upgrades an anonymous ordered stream with
`Noise_XX_25519_AESGCM_SHA256`. Its Noise prologue is the UTF-8 byte string
`netz/kabel-noise/v1\0`. Each physical connection generates a fresh X25519
static key as well as the ephemeral key required by Noise XX.

Noise message 1 carries the initiator's deterministic-CBOR offer:

~~~text
{
  "v": 1, "type": "netz/noise/offer",
  "capabilities": [<sorted unique text>, ...],
  "required": [<sorted unique text>, ...]
}
~~~

Capability arrays are sorted by ascending byte value. The address rules are
defined in Section 6. Visible ASCII makes ordering byte-identical in JVM,
JavaScript and Unicode-codepoint-based foreign runtimes; implementations MUST
NOT expose host-language string collation on this wire surface.

Noise messages 2 and 3 carry the responder's and initiator's identity payloads
respectively. An identity payload contains the signed identity record, the
sender's Noise static public key, its role, supported and required capabilities,
the selected capabilities, and an additional Ed25519 signature by the record's
operational key. That signature covers:

~~~text
UTF8("netz/noise-static-binding/v1\0") || deterministic-CBOR({
  "v": 1,
  "protocol": "Noise_XX_25519_AESGCM_SHA256",
  "authority": <32-byte Authority ID>,
  "noise_static": <32-byte X25519 public key>,
  "role": "initiator" | "responder",
  "capabilities": [<sorted unique text>, ...],
  "required": [<sorted unique text>, ...],
  "selected": [<sorted unique text>, ...]
})
~~~

The identity record contains the genesis object, full Authority ID,
operational Ed25519 public key, sorted addresses, sequence and identity-record
signature. Its existing `netz/identity-record/v1\0` signature MUST verify, its
operational key MUST occur in the genesis, and its Authority ID MUST equal the
full hash derived from that genesis. The `noise_static` value MUST equal the
static key authenticated by Noise. An initiator with an expected dial target
MUST compare the complete 32-byte Authority ID; a routing projection is not an
authentication target.

`selected` MUST be the exact intersection of the two supported sets. Both
peers' required sets MUST be subsets of it. A peer MUST abort rather than
silently choose a smaller set. The initiator's message-3 capability values MUST
also equal its transcript-hashed message-1 offer. Example feature identifiers
include `dissemination/1`, `content/1` and `sync/1`; the direct carrier is
`transport/kabel-noise-cbor/1`.

The connection enters authenticated state only after the Noise handshake,
identity record, static-key binding, expected target and capability selection
all verify. Failure closes the stream and publishes no remote identity to
application middleware. Handshake frames after completion and transport frames
before completion are protocol errors.

### 7.1 Direct Kabel carrier framing

The WebSocket carrier uses Kabel's binary serializer id `0` for the protected
container. The first payload octet is visible carrier framing: `1` denotes a
Noise handshake message and `2` a Noise transport ciphertext. No application
or serializer metadata occurs outside the ciphertext after the handshake.

The decrypted transport plaintext begins with one fragment octet followed by
bytes from one complete Kabel envelope (four-byte serializer id plus payload):

| Fragment octet | Meaning |
|---:|---|
| `0` | complete envelope |
| `1` | first fragment |
| `2` | middle fragment |
| `3` | final fragment |

Fragments MUST remain contiguous on the ordered stream and MUST reconstruct no
more than the 5 MiB Kabel envelope limit. Nested starts, an isolated middle or
end, a new complete envelope during assembly, or closure with an incomplete
envelope are protocol errors. Fragment headers are inside Noise AEAD. This
layering preserves Kabel's 5 MiB envelope limit despite Noise's 65,535-byte
message limit. Profile 1 accepts serializer id `14` (CBOR) after decryption;
any other serializer is an unnegotiated protocol error.

One serialized owner MUST advance each directional Noise CipherState. A failed
authentication, replay, nonce/state reuse or out-of-order ciphertext closes the
connection. Publication signatures remain necessary end-to-end origin proofs;
Noise authenticates only this direct link.

## 8. Dissemination and recent-message repair

A signed publication contains:

~~~text
{
  "v": 1, "type": "gossip", "topic": <text>,
  "origin": <32 bytes>, "epoch": <uint64>, "seq": <uint64>,
  "payload": <bytes>
}
~~~

Its Ed25519 signature covers the deterministic encoding of that map prefixed
with `netz/gossip/v1\0`. Forwarding metadata (`hops`, previous transport peer)
is outside the origin signature and MUST be locally bounded.

The complete self-verifying envelope adds:

~~~text
{
  "genesis": <Genesis>, "operational": <32 bytes>,
  "signature": <64 bytes>, "hops": <uint64>,
  "repair": true                         # optional
}
~~~

The genesis MUST derive `origin`, `operational` MUST occur in its operational
key set, and the signature MUST verify before authorization, deduplication, or
forwarding. `repair` may only be the value `true`; absence means false. Relays
may increment `hops` and set `repair`, but MUST NOT modify a signed field.
`fixtures/publication-1.json` pins the signing bytes, Ed25519 signature, and
complete deterministic envelope. JVM, JavaScript, and Python consume the same
values.

The topic's application profile defines `payload`. A CBOR application may use
the full registered-tag vocabulary accepted by that profile; a non-CBOR
application may use another deterministic or self-describing encoding. Relays
handle the payload as bytes in either case.

`seq` is monotonically increasing within `(origin, epoch, topic)`. Peers
deduplicate and repair by that same stream key. They periodically exchange
`have` summaries containing bounded topic-stream interval sets and repair-store
horizons. `want` requests name bounded missing ranges. If a missing sequence is
below a peer's horizon, the subscriber enters `desynchronized` and opens a
state-sync session.

A subscriber MUST release publications in ascending sequence order within one
origin/topic stream. It MUST NOT infer a total order across origins. A bounded
out-of-order suffix waits while recent-message repair requests the missing
prefix. Buffer overflow triggers state synchronization.

The subscriber state machine is:

~~~text
joining -> synchronized -> desynchronized -> synchronizing -> synchronized
                 ^                                  |
                 +------------- abort --------------+
~~~

Live deltas received while joining or synchronizing MUST be buffered by stream
sequence. A successful checkpoint identifies the stream frontiers represented
by the committed state; buffered values at or below those frontiers are
subsumed and the contiguous suffix above them is released.
Overflow MUST restart state synchronization; it MUST NOT be treated as a gap
that recent-message repair can necessarily observe.

An ordered WebSocket or Kabel connection does not remove this requirement. A
publication can arrive through several relays, after reconnection, or from an
asynchronous producer that completed work out of order before enqueueing it.
Transport FIFO orders one connection only; publication sequence and the
application commit lane define the end-to-end order.

## 9. State synchronization

Synchronization is point-to-point, acknowledged and backpressured. It is not
flooded through the dissemination mesh.

Either peer may open a session. Every frame below also carries `v: 1`:

`StreamFrontiers` is a bounded array, because profile-1 maps permit only text
keys:

~~~text
[{origin: bytes32, epoch: uint64, topic: text, through: uint64}, ...]
~~~

Entries MUST be unique and sorted by origin bytes, then epoch, then UTF-8 topic
bytes. A receiver MUST reject a foreign topic, duplicate stream, unsafe integer,
or more than 1,024 entries. The deterministic-CBOR encoding of the complete
array is limited to 256 KiB. Although these counters use CBOR unsigned-integer
encoding, values above 2^53-1 are invalid so every conforming JavaScript binding
can represent them exactly.

~~~text
sync/open  {session: bytes16, topic: text, profile: text, summary: bytes}
sync/chunk {session: bytes16, index: uint64, data: bytes}
sync/ack   {session: bytes16, through: uint64}
sync/done  {session: bytes16, count: uint64, topic: text,
            checkpoint: bytes, frontiers: StreamFrontiers}
sync/abort {session: bytes16, code: text}
~~~

The `summary`, `data` and `checkpoint` fields are opaque to Netz and defined by
the named profile. A receiver MUST apply chunks in index order and acknowledge
bounded batches. `done` becomes visible atomically through `SyncProfile.commit`;
failure before commit leaves the previous checkpoint visible. The `frontiers`
field is a sender claim, not a receipt. Buffered publications are subsumed only
by the bounded frontier returned from the receiver's successful local commit.
Bindings for legacy profiles MUST return an empty frontier.

`fixtures/sync-open-1.json` pins a complete join-semilattice `sync/open` frame.
The profile summary is itself deterministic CBOR, carried as an opaque byte
string by the outer frame. JVM, JavaScript and Python MUST emit the fixture's
exact bytes.

`fixtures/sync-done-1.json` pins the corresponding canonical frontier-array
shape in a complete `sync/done` frame. It is shared by the JVM, JavaScript and
Python conformance tests.

The Clojure reference keeps profile state as idiomatic maps internally and
uses `netz.wire/sync-frame->wire` and `sync-frame-from-wire` as a strict
boundary. These functions require opaque profile fields to be byte strings and
convert the internal frontier map to the canonical array; they never stringify
arbitrary Clojure values. Transport adapters MUST cross this boundary only
after negotiating profile 1. The native Kabel runtime currently applies this
boundary to named extension traffic; its base overlay, dissemination, content,
and synchronization frames remain transitional native values, so the existence
of these codecs is not yet a complete runtime interoperability claim.
The Python reference exposes the same pure receiver plus a bounded sender that
accepts exact `sync/open` frames, enforces the cumulative-ACK chunk window, and
emits `sync/done` only after all transmitted chunks are acknowledged.

The reference implementation includes executable models of two initial
profiles:

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

The baseline binding is one CBOR message per binary WebSocket message. Both
sides MUST cap the WebSocket message before CBOR decoding. WebSocket supplies
reliable ordered delivery for handshake and sync sessions; dissemination
remains deduplicated because peers may receive the same signed publication over
multiple connections.

The Kabel binding prepends its four-byte big-endian serialization identifier;
CBOR is identifier `14`. Its capability name is `transport/kabel-cbor/1`,
distinct from the prefix-free `transport/websocket-cbor/1`. The CBOR item after
that prefix follows the same Netz envelope and limit rules. The prefix is a
transport discriminator, not part of any signed object.

Dial policies MUST restrict schemes and embedded credentials. Public services
SHOULD reject loopback, link-local and private destinations learned from
untrusted peer exchange, and MUST defend against DNS rebinding and redirects at
the actual socket layer. Configured LAN seeds may use a separate explicit
policy.

Future QUIC, WebTransport or libp2p bindings MUST preserve the same identified
connection, bounded-send and ordered-sync semantics.

The capability `wire/netz-cbor/1` selects the strict text-key profile in this
document. It is distinct from the transport capability and MUST be negotiated
before a Clojure implementation replaces its transitional native Kabel
control values. Implementations MUST NOT reinterpret the older `netz/1`
capability as this wire profile; doing so would make an existing connection
appear compatible while changing every decoded message shape.

### 11.1 Portable extension carrier

An authenticated Kabel connection that negotiated `wire/netz-cbor/1`,
`netz/extension-cbor/1`, and a named extension capability carries extension
messages as the CBOR array:

~~~text
["netz/extension-cbor/1", <envelope-bytes>]
~~~

`envelope-bytes` is a byte string containing the Core Deterministic Encoding
of this closed map:

~~~text
{
  "v": 1,
  "type": "netz/extension",
  "capability": <the exact negotiated capability>,
  "payload": <the extension frame>
}
~~~

The array marker is reserved on a connection that selected the carrier
capability. `netz/extension-cbor/1` is independently negotiated so older peers
that already advertise the wire and application-extension capabilities keep
their prior framing rather than being silently reinterpreted.

The outer array belongs to the general Kabel CBOR carrier, not the deterministic
profile-1 control value. Producers SHOULD use its preferred deterministic
encoding pinned by the fixture, but receivers MUST accept any well-formed CBOR
value that decodes to exactly the marker text and envelope byte string, including
non-minimal lengths and indefinite arrays/strings. This matches Kabel decoders
that expose the decoded carrier value. The inner envelope bytes MUST use Core
Deterministic Encoding and remain the cross-language conformance boundary.

The receiver MUST reject a malformed carrier, non-deterministic inner bytes,
unknown field, invalid capability name, capability not selected by the Noise
transcript, unknown local handler, or handler-invalid payload by closing the
connection. The carrier uses the same authenticated connection identity,
rate-limit, bounded queue, and disconnect lifecycle as native overlay frames.

The inner byte string prevents Kabel's host-language CBOR options from becoming
the canonicalization boundary. The typed outer array, rather than a naked byte
string, leaves ordinary application byte values unclaimed. Its preferred
serializer-14 payload is pinned by `fixtures/extension-envelope-1.json`. A
transitional Clojure peer without the carrier capability continues to use the
native keyword-map wrapper; independent implementations MUST require all three
capabilities and MUST NOT accept that wrapper.

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
