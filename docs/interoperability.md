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

- deterministic CBOR and CID-compatible content identifiers;
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

The first independent seam is now concrete: strict Clojure sync-frame
conversion and the installable `netz-protocol` Python core emit the same
`sync/open` and `sync/done` fixtures. This proves the CBOR and frontier shapes,
not a network connection. Identity/handshake fixtures, capability negotiation,
outbound Python sync, and two-peer tests remain prerequisites.
