# Secure transport and opaque storage replication

Status: design note. This is not yet a wire commitment.

Netz needs two independent encryption boundaries:

1. a mutually authenticated session below Kabel serialization, protecting every
   byte on one live connection; and
2. optional object/store encryption whose ciphertext may remain opaque across
   synchronization and relays.

Conflating them would make a transport reconnect change the stored object format,
or make a Konserve codec choice part of peer authentication. Neither is useful.

## Related protocols

Matrix Olm solves asynchronous, store-and-forward one-to-one messaging. Its
one-time prekeys and Double Ratchet provide offline initiation, per-message
forward secrecy and post-compromise recovery. Megolm trades some of those
properties for efficient group distribution. Netz currently has an ordered,
interactive Kabel connection, so importing either ratchet would add persisted
session state and skipped-message-key handling without solving an additional
release requirement.

Noise XX is a closer fit. libp2p uses it to upgrade an anonymous ordered stream
to a mutually authenticated encrypted connection before multiplexing application
protocols. The selected suite is X25519, ChaCha20-Poly1305 and SHA-256, and the
peer's long-term identity signs its Noise static key. The important architectural
property is the placement: all later bytes use the resulting transport keys.

TLS protects a WebSocket hop but is not by itself the Netz identity protocol.
Ordinary WSS authenticates the server DNS name, does not authenticate both Netz
peer identities, and the current Kabel adapters do not expose a TLS exporter that
could bind the Netz proof to that exact connection.

## Decision

Use a Noise XX transport upgrade below Kabel's serializer rather than
standardize a new overlay-level signed-DH construction. The discarded
prototype helped identify three disadvantages:

- it defines a new authenticated-key-exchange construction where Noise already
  supplies a reviewed one;
- it protects only Netz overlay maps, leaving other Kabel pub/sub/application
  values on the same connection outside the session; and
- it must serialize a decoded host-language value again, entangling connection
  security with Clojure-native CBOR tags and custom handlers.

The prototype was removed from the release branch after review also found that
its asynchronous send path could reuse a sequence-derived AES-GCM nonce under
concurrent calls. A serialized, vector-tested Noise transport owner replaces
it; no custom session construction is part of profile 1.

## Proposed secure binding

Protocol name:

```text
Noise_XX_25519_AESGCM_SHA256
```

AES-GCM is selected instead of libp2p's ChaChaPoly suite because AES-256-GCM is
available through the same Web Crypto API on JVM, browsers and Node. It is a
standard Noise cipher suite and already has byte-identical JVM/CLJS primitives in
Geheimnis. ChaChaPoly can be added as a separately named binding when the browser
implementation is equally dependable; cipher negotiation must never occur inside
one protocol name.

Each connection generates fresh Noise `s` and `e` X25519 keypairs. A handshake
payload contains:

- the signed Netz identity record;
- an Ed25519 signature by its operational key over a domain-separated tuple of
  the Noise protocol name and the sender's Noise static public key;
- the offered Netz protocol/profile capabilities; and
- optional transport extension values.

The identity record first proves that the operational Ed25519 key belongs to the
full 32-byte authority anchor. The additional signature binds that identity to
the Noise handshake. Noise encrypts the static key and payload and incorporates
all handshake messages into its handshake hash, preventing replay, transcript
splicing and unknown-key-share substitution.

After message 3, the connection is registered only if:

- the Noise handshake and AEAD tags verified;
- the identity record verified;
- its full authority anchor matches the expected dial target, when one exists;
- the operational signature over the Noise static key verified; and
- capability selection produced a supported profile.

All subsequent Kabel bytes are Noise transport messages. The Noise transport
nonce is monotonic per direction; a connection closes on authentication failure
or nonce exhaustion. Netz retains its publication signatures because those are
end-to-end origin proofs that survive relaying, whereas Noise authenticates only
one link.

The reference binding carries each protected container as Kabel serializer id
`0` with a one-byte carrier kind. After the handshake, the ciphertext decrypts
to a fragment kind followed by the original four-byte serializer id and encoded
payload. Fragmentation occurs *inside* AEAD, allowing Kabel's bounded 5 MiB
envelope even though one Noise message is limited to 65,535 bytes. The initial
profile accepts only serializer id `14` after decryption, so a peer cannot
silently switch the negotiated CBOR carrier to a host-native codec.

The implementation is `netz.transport.noise/middleware`. Until the Kabel and
Geheimnis changes are released, `:secure-test` pins their reviewed Git commits;
the published Netz runtime dependency graph continues to use released
coordinates.

Fresh per-connection static keys avoid persistent DH secret storage and give
connection forward secrecy once handshake key material is erased. Rekeying is a
new connection for the first release. Matrix-style ratcheting can remain an
application/content profile when asynchronous or post-compromise-secure messaging
actually needs it.

## Kabel API work

Kabel needs a distinct raw transport-upgrade stage:

```text
WebSocket bytes
  -> transport middleware (Noise)
  -> serializer middleware (CBOR id 14)
  -> application middleware (Netz/pubsub)
```

Each physical connection also needs a context containing at least:

- a unique connection id;
- `:initiator` or `:responder` role;
- expected dial target, if any;
- authenticated remote authority and routing id after the upgrade; and
- negotiated protocol/capabilities.

Kabel should invoke these stages separately and preserve the context even when a
legacy middleware returns the existing three-element tuple. This can be added
without changing CBOR id 14 or the peer/topic pub/sub API. Netz then consumes the
authenticated context and removes its duplicate application-level hello/proof.

## Python and other implementations

Foreign clients can use an existing Noise implementation with the exact named
suite, then exchange the ordinary Kabel CBOR byte stream inside Noise transport
messages. Conformance fixtures need to pin:

- all three Noise handshake messages with deterministic test keys;
- the identity-to-Noise-static signature bytes;
- the handshake hash and split transport keys;
- the first transport ciphertext in each direction; and
- malformed signatures, messages, tags and nonce exhaustion.

Profile 1 control objects remain deterministic text-key CBOR. Application objects
and storage blobs remain byte strings; their own CBOR tags are not constrained by
the control-envelope profile.

## Konserve raw-blob synchronization

Current `konserve-sync` synchronizes logical values: it calls `get`/`bget` at the
source and `assoc`/`bassoc` at the destination. Each destination therefore
serializes, compresses and encrypts again. This is correct across different store
configurations, but it is not a raw copy and an intermediate sync endpoint sees
the logical value.

A separate raw-blob mode is useful, especially for immutable Datahike index nodes,
bulk bootstrap, backup and read-through caches. Its compatibility decision belongs
to Konserve, not Netz. Netz should carry the blob as opaque bounded chunks.

A raw transfer is valid only when the destination declares that it can import the
source blob unchanged. At minimum the offer describes:

- Konserve layout version and complete blob byte length/hash;
- serializer, compressor and encryptor identifiers from the blob header;
- the logical Konserve key to which AEAD associated data is bound;
- an application-supplied encryption-domain identifier, when encrypted; and
- immutable/content-addressed versus mutable semantics.

For PR #150's AES-GCM format, an unchanged ciphertext can be copied to the same
logical key when both stores share the master key and layout version. A different
master key requires decrypting and re-encrypting and is therefore a logical or
transforming transfer, not a raw one. The encryption-domain identifier must not be
the secret key; it is an opaque deployment identifier whose equality authorizes
attempting the copy. Successful destination authentication remains the final
check.

Immutable content-addressed blobs are the safe first profile: stage the complete
blob, verify its transport hash and Konserve readability/authentication, then
atomically install it if absent. Mutable root records continue through the
application/profile state machine, where writer fencing, version comparison and
commit receipts are explicit. Copying a physical `last-write` timestamp must not
become conflict resolution.

Mixed serialization is allowed. A store can raw-copy an existing Fressian blob
and later write new Boring/CBOR blobs because Konserve records the serializer in
each header. Synchronization should not silently translate old blobs merely to
make CBOR the new default.

## Release order

1. Rebase Konserve PR #150 onto current main and land its authenticated at-rest
   format independently.
2. Add Kabel connection context and raw transport-upgrade stages.
3. Implement the standard Noise XX state machine in Geheimnis using its existing
   X25519, HKDF, AES-GCM and SHA-256 primitives, including official Noise vectors.
4. Bind Netz identities/capabilities in the Noise handshake payload and delete the
   application-level hello/proof path.
5. Add JVM/CLJS/Python cross-implementation and active-relay tests.
6. Add Konserve raw import/export capabilities, then a `konserve/blob/1` sync
   profile for immutable blobs.
