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

`netz_protocol.replication` implements transport-independent deterministic
placement for the experimental durable-blob extension. The companion
`netz_protocol.replication_wire` module validates the frozen retention, lease,
deletion, replica-receipt and bounded transfer-frame schemas, including the
authenticated-provider fetch request. `netz_protocol.replication_layout`
validates and addresses the signed replica layout, reconciles its exact
predecessor chain, freezes on controller forks, and checks deterministic target
admission. Python and Clojure share the placement, blob-manifest and
signed-statement known-answer corpora. `netz_protocol.materialization`
validates and addresses the same closed
application-key/object binding as the JVM/JavaScript implementation. The
`netz_protocol.blob_store.FileBlobStore` adds asynchronous fsync or
best-effort piece staging, atomic installation markers, restart/resume,
inventory, scrubbing, and bounded reads. Its piece paths and deterministic
CBOR markers are byte-compatible with the JVM file store. The Python transfer
machine/runtime and Konserve adapter are not yet part of the API.

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

The filesystem repository has no third-party dependency:

```python
from netz_protocol import BLOB_STORE_FSYNC, FileBlobStore, split_blob

split = split_blob(b"encrypted application bytes", piece_size=64 * 1024)
session = bytes.fromhex("00" * 16)
async with FileBlobStore("./netz-blobs") as store:
    for piece in split["pieces"]:
        await store.stage_piece(
            session, split["manifest_id"], split["manifest"],
            piece["index"], piece["id"], piece["data"])
    await store.install_object(
        session, split["manifest_id"], split["manifest"],
        retention={}, persistence=BLOB_STORE_FSYNC)
```

The session argument is transport bookkeeping and never enters a path. One
process must exclusively own a repository root. An installation result is a
local persistence claim, not a signed replica receipt or a replication quorum.

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

Run the complete Python conformance suite from the repository root:

```sh
python3 -m unittest discover -s interop/python -v
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
