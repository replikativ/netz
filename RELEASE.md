# Release readiness

Target: `io.replikativ/netz` `0.1.0-alpha.1` under Apache-2.0.

## Completed in the alpha hardening pass

- [x] License and Maven/npm metadata agree on Apache-2.0.
- [x] Decoded message shapes, collection sizes, strings and nesting are bounded.
- [x] Unknown overlay message types are rejected before negotiation exists.
- [x] Application delivery overflow escalates to state synchronization.
- [x] New identities persist only hot material in the ordinary peer store.
- [x] Runtime has idempotent shutdown and adapter cleanup.
- [x] Dialing has an explicit operator policy and safe ws/wss syntax default.
- [x] Identity handshakes are attempt-limited, deadline-bound and complete once.
- [x] Subtree sessions have deadlines and retry alternate providers.
- [x] JVM, browser/Node, optional store, Python fixture and jar build are CI gates.
- [x] A language-neutral draft and deterministic-CBOR fixture exist.

## Required before exposing to untrusted peers

- [ ] Kabel must route inbound direct publications through the selected pub/sub
      transport, or reject mixed direct/overlay operation. Until then the two
      publication paths can partition.
- [ ] Kabel's WebSocket adapters need bounded asynchronous writes or byte
      credits. Netz cannot bound a queue below the transport that owns it.
- [ ] The actual socket layer must enforce the 5 MiB encoded-frame ceiling and
      public deployments need DNS/private-address/redirect policy.
- [ ] Subtree disconnect handling should retry immediately rather than waiting
      for the bounded deadline.
- [ ] Root records need an independently verifiable publisher/signature binding.
- [ ] Rotation-chain and revocation distribution must be implemented or remain
      explicitly unsupported in every advertised protocol version.

## Required before calling wire profile 1 interoperable

- [ ] Encode/decode current runtime messages through `netz.wire` rather than
      native Clojure values.
- [ ] Add canonical identity, signature and malformed-input fixtures.
- [ ] Implement capability/version negotiation.
- [ ] Complete a Python identity/handshake/dissemination/sync client.
- [ ] Pass independent JVM/JS/Python two-peer conformance tests.
- [ ] Pass a real three-or-more-peer partition/rejoin test for both a fenced
      single-writer root and a multiwriter join-semilattice.

## Artifact commands

```sh
clojure -X:test :dirs '["test"]'
clojure -X:konserve:test :dirs '["test" "test-konserve"]'
npm ci
npx shadow-cljs compile :node-test
node target/node-tests.js
python3 -m unittest discover -s interop/python -v
clojure -M:format
clojure -T:build jar
```

Deployment is intentionally not part of CI. `clojure -T:build deploy` requires
explicit Clojars credentials and a human-selected release tag.
