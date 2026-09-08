# Interoperability notes

Netz should adopt established encodings and concepts where semantics align,
but wire similarity alone is not compatibility.

## AT Protocol

[AT Protocol synchronization](https://atproto.com/specs/sync) is the closest
deployed analogue for the single-writer profile: signed content-addressed
repository commits, a live WebSocket/CBOR event stream, sequence-gap detection,
and full repository resynchronization. Its repository format uses signed
commits over a content-addressed Merkle Search Tree and CAR exports.

Useful points to reuse:

- deterministic CBOR and an exact CIDv1/raw/SHA-256 content profile;
- an explicit synchronized/desynchronized state;
- signed root records plus live deltas and full resync;
- a hard encoded-frame ceiling and validation before application.

Netz is not an AT Protocol transport. AT repositories have an authoritative
PDS and account/DID model; Netz must also support symmetric multiwriter CRDT
state, configurable peer authority, and offline peer-to-peer exchange. An AT
repository adapter can be a `root-log` synchronization profile without making
AT schemas or relays mandatory in the core.

## libp2p

[libp2p](https://github.com/libp2p/specs) offers a broad, multi-language
networking stack: transport negotiation, secure channels, multiplexing,
relays, peer discovery, GossipSub, and Kademlia. Netz currently occupies a
narrower layer on Kabel/WebSockets and additionally specifies how delivery
failure escalates into application state synchronization.

The draft should allow a future libp2p transport binding, but replacing Kabel
is not necessary for the first release. Such a binding must preserve Netz's
identified connection, bounded-send, ordered point-to-point sync, and
application backpressure semantics. Mapping publications to GossipSub alone is
insufficient because GossipSub does not define the application checkpoint and
resynchronization contract.

libp2p is not one Go library. It is a family of specifications and independent
Go, Rust, JavaScript/TypeScript, Python, Nim, Kotlin/JVM and other
implementations. `js-libp2p` supports Node and browsers; `jvm-libp2p` is Kotlin
and produces ordinary JVM artifacts. Adopting a libp2p binding therefore does
not imply shipping Go or a native library everywhere. It does imply selecting
and testing a compatible subset: the implementation-status matrix differs by
transport and protocol, particularly on the JVM.

For Netz, the useful integration points are explicit:

- a libp2p stream can carry the same profile-1 Netz control envelopes;
- libp2p Identify, relay, rendezvous, Kademlia, WebRTC and WebTransport can
  replace or augment discovery and reachability;
- GossipSub can replace the eager flooding path if it preserves the signed
  publication bytes and Netz repair/frontier semantics; and
- Netz synchronization remains an application protocol above the stream.

The first release should not make libp2p mandatory. Kabel already gives the
stack a small functional middleware model, browser-friendly WebSockets, JVM/JS
code sharing, and direct integration with the existing sync strategies. A
mandatory libp2p dependency would enlarge the artifact and duplicate identity,
negotiation and pub/sub machinery without supplying the state-commit contract
that distinguishes Netz. The draft instead keeps the transport boundary open
and treats a libp2p binding as a later interoperability profile.

## Matrix

[Matrix federation](https://spec.matrix.org/latest/server-server-api/) is a
useful comparison for durable collaborative history. Homeservers exchange
signed persistent events, backfill missing history, retrieve state, and apply a
room-version-specific state-resolution algorithm when an event graph forks.
It also separates persistent PDUs from ephemeral EDUs.

The reusable lessons are explicit missing-event retrieval, signed events that
can travel through third parties, versioned conflict semantics, and the split
between durable and ephemeral traffic. Matrix is not a direct Netz carrier:
its unit of authority is a federated homeserver and room event graph, its
protocol surface includes the full room/auth/state model, and its federation
uses HTTPS transactions rather than symmetric offline peer sessions. A Matrix
room adapter could be a synchronization profile, but importing Matrix state
resolution into the Netz core would conflict with application-defined CRDT and
fenced-writer profiles.

## Why the genesis record is retained

A raw public key is a simpler identifier, but it makes key compromise terminal:
changing the key changes the identity. Netz hashes a small genesis record that
commits at creation time to separate cold rotation keys, hot operational keys,
a threshold, and a revocation-secret commitment. The stable Authority ID can
therefore survive a future verified rotation without a directory lookup or a
wire-format migration.

This design has a cost: every self-verifying publication currently carries the
genesis and operational key. It is acceptable for draft-00 and makes relaying
independent of prior contact. A negotiated connection may later cache verified
identity material and carry a compact reference, but that optimization must not
make a relay trust discovery as authority. Rotation and revocation messages are
reserved and rejected until their chain and distribution rules are specified.

## Willow

[Willow](https://willowprotocol.org/specs/index.html) is a stronger conceptual
match for capability-aware, partial, multiwriter synchronization. Its
specification family includes a data model, Meadowcap capabilities,
confidential synchronization, reconciliation, flow control, and encrypted
entries.

Netz should study Willow's range reconciliation, capability separation, and
privacy properties before inventing equivalents. Direct wire compatibility is
not immediate: Willow defines its own data model and parameter sets, whereas
Netz deliberately delegates state semantics to profiles. A Willow-backed
profile is more plausible than merging Willow into the base protocol.

## Kademlia and discovery

The [libp2p Kademlia specification](https://github.com/libp2p/specs/blob/master/kad-dht/README.md)
separates peer routing, value storage, and content-provider records. Netz's
current bounded peer exchange and two-hop provider discovery satisfy the known
stack deployments without DHT maintenance, Sybil exposure, or browser-hostile
background traffic.

Kademlia should remain an optional routing provider until measurements show
that bounded discovery cannot find content or peers at the intended scale. It
must never become an authorization source.

## Compatibility gate

The CBOR profile is a target, not yet a compatibility claim. A second
implementation is interoperable only after it agrees on canonical bytes and
signatures, rejects the shared malformed corpus, negotiates protocol versions,
and completes identity, dissemination, repair, content, and state-sync tests
against the JVM/JavaScript implementation.

The independent seam is now concrete: strict Clojure codecs and the installable
`netz-protocol` Python core emit the same identity, Noise binding, signed
publication, `sync/open`, and `sync/done` fixtures. The Python package also has
a real secure WebSocket round trip, bounded ordered publication receiver, and
one-session durable-blob sender/receiver. JVM, JavaScript and Python pin the
same canonical need/piece/ACK/commit trace. This proves the byte, signature,
carrier, receiving-session and blob flow-control shapes. Named extensions now
also have an exact capability-bound carrier fixture and the JVM runtime selects
it after separate wire-profile and carrier negotiation; Python exposes both
halves of that codec. The separately negotiated `netz/sync/1` path now carries
strict portable sync frames, and real Python/JVM peers complete atomic
join-semilattice pulls in both directions. Portable sync support is not read
authorization: the JVM runtime rejects incoming opens by default and its
`:authorize-sync?` hook receives the authenticated connection facts, topic and
profile. The negotiated carrier is pinned for the session, preventing native
schema downgrade or reinterpretation across reconnects. The dissemination wire
boundary is now closed too: JVM, JavaScript and Python share deterministic
exact-topic `interests`, flattened full-Authority `have`, bounded `want`, and
signed opaque publication frames under `netz/dissemination/1`. A complete
portable runtime now selects that grammar only with the wire, named-carrier and
dissemination capability triple, rejects native downgrade and Authority-routing
collisions, and applies bounded fail-closed policy before entering the pure
machine. The separately negotiated `netz/content/1` service and requester also
pass authenticated raw-CID fetches in both Python/JVM directions, with the
receiver independently verifying the returned bytes. Both Python directions
now use the reusable reconnecting client rather than a one-shot carrier. The
same mixed-runtime gate also compiles a Node/ClojureScript peer and checks a
real Node-to-JVM fetch through Kabel WebSocket, CBOR and authenticated Noise.
Its configured seed pins the full server Authority, capability negotiation
selects the portable content carrier, and the requester verifies the raw CID.
Run `npx shadow-cljs compile mixed-content` before running `:mixed-test` locally.
This adds a live JavaScript requester, not yet JavaScript serving, sync,
dissemination or a simultaneous three-runtime topology. The
JVM/JavaScript node layer additionally ingests authorization-first provider
announcements and performs capped, capability-filtered two-hop discovery over
already connected peers; it never implicitly dials or elevates a hint to
authority. Python can compose its complete single-peer clients under the same
multi-peer policy, but does not yet ship that node orchestrator. A dedicated
conformance process now drives a real Python
blob sender through Noise/Kabel and the negotiated portable carrier into the
JVM fsync receiver, including verified and durably persisted receipt evidence.
The reverse authenticated fetch drives the JVM sender into Python's fsync
repository and returns its signed receipt to the JVM journal. Its minimal
test-only controller fsyncs the exact admitted retention envelope before the
fetch, and the JVM provider separately pins the authorized requester, cluster,
manifest and layout epoch. The mixed test also restarts the Python process after
the first piece is durable but before ACK; the new session requests only the
missing tail. A reusable Python lifecycle/carrier runtime remains future work.

## Boring and the CBOR data model

Netz does not need a private tag-free dialect. Boring emits RFC 8949 CBOR and
its normal semantic tags are registered: identifiers (`39`), generic objects
(`27`), sets (`258`), string references (`25`/`256`), UUIDs (`37`), CIDs (`42`),
and RFC 8746 typed arrays. Foreign implementations may preserve an unfamiliar
registered tag as a tagged value or install a small application-level hook.

The one provisional Boring tag is shaped arrays at `39649`. It is opt-in and
off in Boring's interop and deterministic profiles. Netz application profiles
may negotiate it, but the baseline profile must not emit it until the number is
registered with IANA.

The narrow `netz.wire` data model belongs to control and signature envelopes.
Application CBOR is carried as opaque byte strings. This preserves unknown tags
and exact signatures across relays and prevents a Python, Rust, or JavaScript
runtime from having to recreate Clojure host types merely to forward a value.

Boring's Python interoperability suite confirms that `cbor2` reads its fixture
with small hooks for the host-specific interpretations. Two implementation
details matter at signature boundaries:

- `cbor2.dumps(..., canonical=True)` uses RFC 7049 length-first key ordering,
  whereas Boring's `:canonical` uses RFC 8949 Core Deterministic bytewise
  ordering. Text-only envelope keys avoid most differences, but a protocol must
  still name one rule.
- The current `cbor2` C encoder fails to choose binary16 for part of its exact
  range. Boring and `cbor2`'s pure-Python encoder agree on the shorter encoding.
  Netz control objects exclude floats, and opaque application payloads are
  signed as bytes, so this does not affect protocol verification.
