# Python secure-client dependency assessment

Status: prototype decision record, not a production endorsement.

## Recommendation

Keep the base `netz-protocol` distribution dependency-free. Provide:

- `netz-protocol[crypto]` for Ed25519 identity creation and verification; and
- `netz-protocol[secure-client]` for the Noise and asyncio WebSocket adapter.

For alpha interoperability, pin `noiseprotocol==0.3.1` behind Netz's own small
adapter. It is MIT licensed and delegates X25519, AES-GCM, and SHA-256 to the
Apache-2.0/BSD-licensed `cryptography` package. Its last release was in 2020,
so this is suitable for conformance and an experimental client, not an
unqualified production security claim.

The important positive result is byte-level compatibility. On Python 3.12 with
`cryptography==50.0.1`, version 0.3.1 reproduces all six messages and the
handshake hash in the public-domain Cacophony
`Noise_XX_25519_AESGCM_SHA256` vector. The same fixture is used by Geheimnis's
JVM and ClojureScript implementation.

## Alternatives considered

`noiseframework==1.3.0` is MIT licensed and was released in November 2025, but
it is a single-maintainer alpha with no external adoption or official-vector
corpus in its repository. More importantly, it does not reproduce the shared
AES-GCM transcript. Inspection shows that its SHA-256 and SHA-512 `hmac_hash`
methods call one-iteration PBKDF2 rather than HMAC; these are different
functions and derive different Noise handshake keys. It also has no public
prologue input in its high-level handshake API. It must not be used for Netz.

`dissononce` supports the named suite but is also old and less widely used.
`py-libp2p` currently uses `noiseprotocol`, but pulling the full libp2p runtime
into the direct Kabel/WebSocket client would add an unrelated transport,
multiplexing, and peer-identity stack. Its Noise profile is also libp2p's
ChaChaPoly binding, not the direct Netz AES-GCM binding.

Writing another Python Noise state machine would remove the old dependency but
would create a second security-critical implementation for Netz to own. That is
not justified while the established implementation passes independent vectors.

## Upstream or fork work

Before describing the Python client as production-ready:

1. Add a public `get_remote_static()` method to `noiseprotocol`. Version 0.3.1
   deletes the handshake state at split and otherwise makes the authenticated
   XX static key unavailable. The prototype has one isolated compatibility
   hook around that cleanup method.
2. Modernize its Python metadata and CI and run the complete official vector
   corpus against supported Python and `cryptography` releases.
3. Enforce the Noise distinction between the 65,535-byte ciphertext limit and
   the 65,519-byte transport plaintext limit in the adapter or upstream.
4. Close a session permanently after any authentication or state error.
5. Seek upstream review. If the project remains inactive, publish a narrowly
   maintained replikativ fork instead of silently vendoring it into Netz.

## Prototype wire choices requiring specification review

The prototype uses prologue `netz/noise/v1`. XX message 1 has an empty payload;
the responder identity is encrypted in message 2 and the initiator identity in
message 3. Each identity payload contains:

- the signed deterministic-CBOR identity record;
- the actual ephemeral-per-connection Noise static public key;
- an operational Ed25519 signature over authority, exact Noise protocol name,
  and that static key; and
- the sorted offered capability identifiers.

The peer verifies the record, expected dial target, static-key signature, and
equality with the static key authenticated by XX before exposing a peer
context.

Noise messages are at most 65,535 bytes, while Netz currently permits a 5 MiB
carrier frame. The prototype therefore treats decrypted Noise payloads as an
ordered byte stream: a four-byte unsigned big-endian inner-frame length followed
by the frame bytes, fragmented across as many 65,519-byte Noise plaintexts as
needed. Concurrent sends are serialized. This framing needs to become normative
before independent implementations depend on it.

The next shared fixture should pin the deterministic identity record, Noise
static-binding transcript and signature, encrypted messages 2 and 3, negotiated
capabilities, handshake hash, and first length-framed transport ciphertext.

## Dependency and license summary

| Package | Proposed range | License | Role | Risk |
|---|---|---|---|---|
| `cryptography` | `>=43` | Apache-2.0 OR BSD-3-Clause | Ed25519, X25519, AES-GCM backend | Mature; native wheels are an expected Python dependency |
| `noiseprotocol` | `==0.3.1` | MIT | Noise revision 32/33 state machine | Correct shared vector, but 2020 alpha and inactive API |
| `websockets` | `>=14,<16` | BSD-3-Clause | Optional asyncio carrier | Mature; major versions intentionally bounded for the prototype API |

All licenses are compatible with Netz's Apache-2.0 distribution. License
compatibility does not reduce the maintenance and security-review risks above.
