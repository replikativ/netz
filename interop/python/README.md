# Python interoperability package

`netz_codec.py` is a dependency-free implementation of the portable,
deterministic CBOR subset in `spec/netz-draft-00.md`. `netz_sync.py` adds the
transport-independent receiving half of an acknowledged synchronization
session plus the reference join-set and fenced-root profiles.

The alpha distribution is named `netz-protocol` (the shorter `netz` name is
already owned on PyPI) and imports as `netz_protocol`:

```python
from netz_protocol import SyncReceiver, dumps, loads
```

Build an installable wheel from the repository root without publishing it:

```sh
python3 -m pip wheel . --no-deps --wheel-dir dist
```

The receiver treats `sync/done.frontiers` as an untrusted sender claim. A
profile commit returns a bounded local `CommitReceipt`; the reference profiles
claim no frontier until their checkpoint formats bind one causally or
cryptographically.

Run the shared fixture tests from this directory:

```sh
python3 -m unittest -v test_netz_codec.py
```

This is intentionally a protocol core, not yet a complete network client. The
session receiver is bounded, ordered and atomic, but a useful client also needs
the final identity/signature fixtures, WebSocket framing, capability handshake,
the outbound half of the sync window, and reconnect policy.
