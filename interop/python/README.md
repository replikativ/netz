# Python interoperability seed

`netz_codec.py` is a dependency-free implementation of the portable,
deterministic CBOR subset in `spec/netz-draft-00.md`. `netz_sync.py` adds the
transport-independent receiving half of an acknowledged synchronization
session plus the reference join-set and fenced-root profiles.

The receiver treats `sync/done.frontiers` as an untrusted sender claim. A
profile commit returns a bounded local `CommitReceipt`; the reference profiles
claim no frontier until their checkpoint formats bind one causally or
cryptographically.

Run the shared fixture tests from this directory:

```sh
python3 -m unittest -v test_netz_codec.py
```

It is intentionally not called a complete Netz client yet. The session core is
bounded, ordered and atomic, but a useful network client also needs the final
identity/signature fixtures, WebSocket framing, capability handshake, outbound
window management and reconnect policy.
