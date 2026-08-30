# Python interoperability seed

`netz_codec.py` is a dependency-free implementation of the portable,
deterministic CBOR subset in `spec/netz-draft-00.md`.

Run the shared fixture tests from this directory:

```sh
python3 -m unittest -v test_netz_codec.py
```

It is intentionally not called a Netz client yet. A useful client also needs
the final identity/signature fixtures, WebSocket framing, capability handshake,
bounded queues and synchronization session state machine. Building those on an
unstable wire would create a second implementation to rewrite rather than an
independent interoperability proof.
