# Python interoperability package

`netz_codec.py` is a dependency-free implementation of the deterministic CBOR
control-envelope subset in `spec/netz-draft-00.md`. `netz_sync.py` adds both
transport-independent halves of an acknowledged, windowed synchronization
session plus the reference join-set and fenced-root profiles. The packaged
`netz_protocol.framing` module implements both raw WebSocket-CBOR frames and
Kabel's four-byte serializer-14 prefix.

`netz_protocol.identity` derives the full 32-byte Authority ID and its compact
routing projection from the same deterministic genesis fixture as the JVM and
JavaScript implementation. Only the full Authority ID is an authentication
identifier.

`netz_protocol.publication` constructs and verifies the complete signed
profile-1 dissemination envelope. Its application payload is an opaque byte
string: relays verify the exact bytes and never need to decode Boring, CBOR, or
an application-specific format.

`netz_protocol.replication` implements the transport-independent deterministic
placement portion of the experimental durable-blob extension. It understands
owner and physical-site failure domains and shares the
`spec/fixtures/blob-placement-1.json` known-answer corpus with Clojure. Transfer
receipts and the Konserve adapter are not yet part of the Python API.

This does not replace a full CBOR implementation for application values.
Application payloads are opaque bytes and may use Boring's registered tags;
Python applications can decode those with `cbor2` and the small tag hooks in
Boring's `interop/read_boring.py`. Netz keeps that dependency optional because
relays and control-only clients do not need to interpret application state.

The alpha distribution is named `netz-protocol` (the shorter `netz` name is
already owned on PyPI) and imports as `netz_protocol`:

```python
from netz_protocol import SyncReceiver, SyncSender, dumps, loads
```

Build an installable wheel from the repository root without publishing it:

```sh
python3 -m pip wheel . --no-deps --wheel-dir dist
```

`SyncSender.from_open(...)` validates an exact open frame, obtains opaque
chunks from the selected profile, enforces the bounded cumulative-ACK window,
and emits `sync/done` only after every chunk was acknowledged. The receiver
treats `sync/done.frontiers` as an untrusted sender claim. A
profile commit returns a bounded local `CommitReceipt`; the reference profiles
claim no frontier until their checkpoint formats bind one causally or
cryptographically.

Run the shared fixture tests from this directory:

```sh
python3 -m unittest -v test_netz_codec.py
```

This is intentionally a protocol core plus a conformance carrier, not yet a
high-level reconnecting application client. Both session roles are bounded;
the receiver is ordered and atomic, and carrier framing, identities, signed
publications and the secure capability handshake are pinned. Subscription
repair, content exchange, discovery policy, and reconnect/backoff remain
explicit application-client work.

The experimental optional secure-client layer can be installed with:

```sh
python3 -m pip install 'netz-protocol[secure-client]'
```

It provides Ed25519 identity helpers, an adapter for
`Noise_XX_25519_AESGCM_SHA256`, and a minimal asyncio WebSocket carrier. The
identity helper canonicalizes signed WebSocket addresses to visible ASCII and
requires visible-ASCII capability tokens, avoiding cross-runtime text ordering
differences. The core package still has no required dependencies. The adapter pins
`noiseprotocol==0.3.1`: that implementation passes the published Cacophony
known-answer vector for this exact suite on current Python/cryptography, but its
last release was in 2020 and its public API does not retain the authenticated
remote static key after an XX handshake. Netz currently uses a narrowly scoped
compatibility hook for that missing API. Treat this as conformance and client
prototyping code until the dependency is adopted, forked, or replaced. The
Noise primitive already shares the exact Cacophony transcript fixture used by
Geheimnis. `spec/fixtures/netz-kabel-noise-binding-1.json` additionally pins the
normative offer, identities, capability selection, authenticated handshake and
fragmented serializer-14 carrier for the JVM/CLJS suite to consume.
