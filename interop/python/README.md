# Python interoperability package

`netz_codec.py` is a dependency-free implementation of the deterministic CBOR
control-envelope subset in `spec/netz-draft-00.md`. `netz_sync.py` adds both
transport-independent halves of an acknowledged, windowed synchronization
session plus the reference join-set and fenced-root profiles. The packaged
`netz_protocol.framing` module implements both raw WebSocket-CBOR frames and
Kabel's four-byte serializer-14 prefix. `netz_protocol.extension` adds the
exact portable named-extension carrier used after `wire/netz-cbor/1` and
`netz/extension-cbor/1` negotiation. Its frame bytes can be passed directly to
`SecureWebSocket.send_frame`; encoding and decoding take the authenticated
peer's selected capability set and require the wire, carrier, and named
extension capabilities before returning the closed profile-1 payload.

`netz_protocol.identity` derives the full 32-byte Authority ID and its compact
routing projection from the same deterministic genesis fixture as the JVM and
JavaScript implementation. Only the full Authority ID is an authentication
identifier.

`netz_protocol.publication` constructs and verifies the complete signed
profile-1 dissemination envelope. Its application payload is an opaque byte
string: relays verify the exact bytes and never need to decode Boring, CBOR, or
an application-specific format. `netz_protocol.dissemination` adds the strict
`netz/dissemination/1` `interests`, flattened `have`, and bounded `want` frames,
plus canonical constructors and a bounded per-stream ordered receiver. The
control schemas use full Authority IDs and share an exact fixture with the JVM
and JavaScript codecs. `netz_protocol.DisseminationSession` composes them into
bounded transport-independent interests, recent history, anti-entropy, repair,
ordered delivery, and state-sync escalation.

`netz_protocol.content` implements the strict `netz/content/1` single-block
profile: CIDv1/raw/SHA-256 construction, bounded provider discovery and
request-correlated transfer. Block contents remain opaque and are verified
before delivery; using this profile does not require an application to encode
its data as CBOR. `netz_protocol.ContentRequester` provides bounded deadlines,
reply correlation and disconnect cleanup. `netz_protocol.ContentService`
authorizes an authenticated peer before consulting its injected block store or
provider index, so policy denial does not become a content-existence oracle.
The reusable asyncio client exposes `fetch_content` and `find_content` on its
live authenticated connection and can install a `ContentService`; transport
policy and storage policy are separate fail-closed gates. Request timeouts,
disconnect cleanup, backend waits, service concurrency, and outstanding state
are bounded. Slow storage does not block dissemination on the shared carrier.

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
CBOR markers are byte-compatible with the JVM file store.

`netz_protocol.BlobSender` and `BlobReceiver` are independent, one-session
transfer machines over the same closed frames as the JVM/JavaScript
implementation. They return `TransferAction` values rather than performing IO.
Receiver actions name staging loads and writes, installation, receipt signing,
frame sends, and cleanup. Sender actions name piece reads, receipt persistence,
frame sends, and the final controller event. The caller must verify and admit a
signed retention statement before constructing a receiver, and must verify the
receipt signature plus authenticated remote Authority before calling
`BlobSender.verified_stored`. A carrier owns the bounded session table,
timeouts, and reconnect policy. There is no reusable Python WebSocket transfer
runtime or Konserve adapter yet; the secure WebSocket, extension codec,
filesystem repository, and pure transfer machines are the separate pieces
needed to build that bounded reconnecting adapter. The separately installed
package is exercised by `test-mixed/python/mixed_blob_sender.py`, an independent
conformance peer: CI runs it through authenticated Noise/Kabel against the JVM
fsync receiver and requires a verified, fsynced receipt before it exits
successfully. `mixed_blob_receiver.py` tests the reverse fetch path through the
Python fsync repository and sends its signed receipt to the JVM journal. Its
minimal test-only controller fsyncs the exact admitted retention envelope before
requesting bytes; it does not replace the reusable lifecycle/carrier runtime
described above. The same test stops one process after a piece fsync but before
ACK, then proves that a fresh process/session requests only the missing tail.
`mixed_sync_receiver.py` independently opens `netz/sync/1` through the same
negotiated carrier and atomically joins a JVM-provided reference state;
`mixed_sync_sender.py` serves the inverse JVM-initiated pull and waits for its
cumulative ACK before emitting `sync/done`. Both now use the reusable client
orchestration. Each provider pins the requester's full Authority ID, topic and
profile; capability negotiation alone never grants access to state.

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

A carrier can drive the pure transfer boundary without exposing its networking
library to application code:

```python
from netz_protocol import BlobReceiver

# Signature, controller, layout, lifecycle, and peer policy already admitted.
receiver = BlobReceiver(
    local_authority, peer_handle, authenticated_remote_authority,
    decoded_offer, now=clock_ms(),
)
for action in receiver.start():
    await execute_transfer_action(action)
```

Each completion is fed back through the specifically named method such as
`staging_loaded`, `piece_staged`, `installed`, or `receipt_signed`. Execute
those transitions serially for a session. Receiving a piece never emits an ACK
until `piece_staged(..., "ok")`; the sender retains no piece bytes after its
send action; and a verified receipt does not reach the controller until
`receipt_persisted("ok")`.

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

Run the mixed Python/JVM transport proof after installing the secure extra:

```sh
NETZ_PYTHON=python3 clojure -X:secure-test:mixed-test :dirs '["test-mixed"]'
```

This is intentionally a protocol core plus a conformance carrier, not yet a
complete general-purpose P2P node. Both session roles are bounded;
the receiver is ordered and atomic, and carrier framing, identities, signed
publications and the secure capability handshake are pinned. The optional
`DisseminationClient` supplies authenticated subscription repair and bounded
reconnect/backoff, durable frontiers, bounded synchronization in both
directions, and direct verified content exchange. Multi-peer provider discovery
and retry remain explicit application work in Python.

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

The same extra provides a fail-closed asyncio dissemination client. Policy is
mandatory and sees the authenticated immediate peer separately from a relayed
publication's signed origin:

```python
import asyncio

from netz_protocol import DisseminationClient, FileFrontierStore
from netz_protocol.secure import LocalIdentity

remote = bytes.fromhex("00" * 32)  # replace with the expected Authority ID

def authorize(context):
    if context["peer_authority"] != remote:
        return False
    return (context["frame_type"] != "gossip"
            or (context["origin_authority"] == remote
                and context["topic"] == "datahike/roots/example"))

client = DisseminationClient(
    identity=LocalIdentity(genesis=my_genesis,
                           operational_private=my_private_key),
    expected_authority=remote,
    topics=("datahike/roots/example",),
    authorize=authorize,
    frontier_store=FileFrontierStore("./netz-frontiers.cbor"),
)

async def main():
    running = asyncio.create_task(client.run("wss://peer.example/netz"))
    delivery = await client.recv()
    await persist_root(delivery.publication["payload"])
    await delivery.commit()  # frontier fsync happens before this returns
    await client.close()
    await running
```

The default `MemoryFrontierStore` is process-local. The optional
`FileFrontierStore` writes canonical CBOR to a temporary file, fsyncs it,
atomically replaces the prior record, and fsyncs its directory. It is a
single-process reference store; a corrupt or non-regular record fails closed
before dialing. `Delivery.commit()` records the authenticated stream and
sequence captured at receipt, not fields in the caller-visible dictionary.

Persist the application value before committing its delivery. If application
storage succeeds but the frontier write fails, the publication may be replayed
and application writes must therefore be idempotent. A committed frontier is
restored before the first `have` announcement. This provides at-least-once
recovery across process restart while retained history can close the gap; it is
not exactly-once delivery. Handle `recv_state_sync()` when history cannot close
a gap, atomically persist the recovered application state, then pass its
per-stream through-sequences to `commit_frontiers()`. Serialize state-sync
commits with application delivery handling.

The same live authenticated connection can perform the repair instead of
opening an unrelated transport stack:

```python
stream, _unavailable_before, _remote_through = await client.recv_state_sync()
repaired = await client.synchronize(stream[2], my_profile, local_state)
local_state = repaired.value
```

`synchronize()` multiplexes `netz/sync/1` beside dissemination, bounds active
sessions and time, authorizes every sync frame with the pinned peer, topic and
profile, sends cumulative ACKs, and aborts on timeout. After the profile has
atomically committed application state, it persists only the frontiers minted
by that local profile before returning. Sender claims remain inert. The
reference join and root profiles intentionally mint no frontiers because their
checkpoints do not yet bind a causal publication position. Set
`require_dissemination=False` only for a sync-only endpoint; dissemination is
required by default.

Incoming serving uses fixed routes and takes its snapshot only after the open
frame has passed authorization:

```python
from netz_protocol import SyncSnapshot, SyncSource

async def snapshot(context):
    value, committed_frontiers = await load_consistent_snapshot()
    return SyncSnapshot(value, committed_frontiers)

client = DisseminationClient(
    # identity, expected_authority, topics, authorize, ...
    sync_sources=(SyncSource("demo/join", my_profile, snapshot),),
)
served = await client.recv_sync_served()
```

The callback receives the authenticated peer, exact topic/profile, direction,
and open frame. Missing routes and callback failures close without revealing
whether application state exists. Snapshots must bind their value and frontier
array consistently; these frontiers are claims sent to the receiver and do not
override the receiver profile's local commit rules. Terminal results report
`synchronized`, `desynchronized`, or `timed-out`. Sender-side `synchronized`
means that `sync/done` was transmitted after all chunks were acknowledged; the
base protocol has no receipt proving the remote application commit.

Publishing is opt-in with `publish=True` and requires an explicitly managed
`epoch`; silently restarting a publisher at sequence zero in the same epoch
would create an ambiguous signed stream.
