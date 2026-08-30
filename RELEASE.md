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
- [x] Point-to-point sync sessions are pure, bounded, acknowledged, atomic at
      profile commit, and exercised with fenced-root and join-semilattice
      reference profiles.
- [x] A pure per-profile apply owner separates admission from successful
      application, serializes snapshot/live effects, bounds item/byte and
      duplicate-receipt backlog, fences apply and sync completions by
      generation/owner attempt, enforces portable integer and frontier bounds,
      and advances only locally bound, application-committed frontiers.
- [x] An optional sibling-workspace gate composes a real Urd/Raft fence with a
      Datahike commit root and heals durable Yggdrasil G-Sets through Spindel's
      delta hook, without leaking local roots into the artifact.
- [x] JVM, JavaScript and Python pin the same canonical `sync/open` and
      frontier-bearing `sync/done` bytes; Python also exercises ordered,
      atomic profile application and pre-commit claim validation.
- [x] Overlay delivery preserves authenticated publication identity through the
      asynchronous application receipt. Only exact local apply success advances
      synchronization frontiers; remote `sync/done` claims are inert unless an
      opted-in local profile validates and returns them. Kabel CBOR supplies
      exact receiver-local frame accounting without relaying it.

## Required before exposing to untrusted peers

- [x] Publication sequence, deduplication, repair, and bounded ordered delivery
      are scoped per origin/topic stream.
- [x] Kabel routes external and direct application through one peer/topic
      serializer and lets Netz reject identity-less direct live frames while
      the overlay owns delivery.
- [x] Horizon and overflow recovery signals are coalesced behind the ordered
      delivery backlog on a one-slot control lane; one worker awaits actual
      snapshot drain for each topic/generation before another attempt can
      start, buffered values are intent/generation-owned, subscription interest
      is rolled back reliably, shutdown cannot open the lifecycle semaphore,
      and terminal failure is observable by status/callback.
- [ ] Move the now-separated delivery receipts and validated snapshot commits
      under one `netz.apply-owner` generation per v1 topic/profile. The runtime
      already prevents slow or failed applies and untrusted `sync/done` claims
      from advancing checkpoints; the remaining gate is generation-fenced
      live/snapshot ownership and receipt settlement for every negotiated
      transport.
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
# With ../urd, ../datahike, ../yggdrasil, and ../spindel checked out:
clojure -X:stack :dirs '["test-stack"]'
npm ci
npx shadow-cljs compile :node-test
node target/node-tests.js
python3 -m unittest discover -s interop/python -v
clojure -M:format
clojure -T:build jar
```

Deployment is intentionally not part of CI. `clojure -T:build deploy` requires
explicit Clojars credentials and a human-selected release tag.
