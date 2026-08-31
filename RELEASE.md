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
- [x] JVM and Python enforce the same deterministic control-envelope scalar,
      depth, item and frame limits; Python requires exact sync frame schemas.
- [x] The Python package pins both raw WebSocket-CBOR and Kabel serializer-14
      carrier framing without folding the carrier prefix into signed objects.
- [x] Clojure exposes a strict internal-map/profile-1 sync-frame boundary,
      validates sender frontier claims before profile code, and computes
      receipt sizes from locally encoded canonical bytes. The dependency-free
      Python core builds as the Apache-2.0 `netz-protocol` wheel.
- [x] Overlay delivery preserves authenticated publication identity through the
      asynchronous application receipt. Only exact local apply success advances
      synchronization frontiers; remote `sync/done` claims are inert unless an
      opted-in local profile validates and returns them. Kabel CBOR supplies
      exact receiver-local frame accounting without relaying it.

## Required before exposing to untrusted peers

- [ ] Bind the identity proof to a cryptographic transport/session and
      authenticate every subsequent Netz control frame. The two-identity,
      two-nonce transcript rejects replay and a chosen-nonce signing oracle,
      but a live adversary can splice the complete public handshake between two
      sockets and then inject unsigned membership/content/sync frames. Candidate
      profile: `Noise_XX_25519_AESGCM_SHA256` below Kabel serialization, with
      serialized transport ownership and Netz identity/capability payloads.
      The direct middleware, exact capability intersection, full-Authority
      binding, encrypted Kabel-envelope fragmentation, JVM/CLJS adversarial
      tests, and a real http-kit/Kabel/CBOR/Noise overlay test now exist behind
      `:secure-test`. `netz.transport.secure/start!` consumes the authenticated
      connection context without the transitional hello/proof exchange. This
      remains unchecked until the Kabel/Geheimnis PRs are released.
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
- [x] Route every Netz-managed Kabel-v0 topic through one `netz.apply-owner`
      generation. Initial and recovery snapshots gate overlay live delivery;
      slow live applies, snapshot completion, queued suffix drain, receipts,
      recovery, unsubscribe, and shutdown are serialized by the owner lane.
      Apply and sync completions are attempt-fenced, and only exact local
      application success advances a publication frontier.
- [x] Kabel 0.3.124 supplies bounded 5 MiB frames/queues and consumes
      `org.replikativ/http-kit 2.9.0-beta4-replikativ.1`, whose server adapter
      bounds queued bytes. Netz consumes the released Kabel coordinate; Jetty
      remains supported and its writes await adapter completion.
- [x] Public gateways can use `public-gateway-address-policy`, which accepts
      only exact operator-configured `(peer-id, wss-address)` pairs and cannot
      be expanded by peer exchange. Dynamic dialing of discovered public
      hostnames remains explicitly unsupported until a socket adapter validates
      and pins resolved IPs; a pre-dial hostname check cannot prevent DNS
      rebinding.
- [x] Subtree disconnect handling retries an alternate provider immediately,
      resets attempt-local counters after partial delivery, and leaves stale
      deadlines unable to complete the replacement attempt.
- [x] Portable root records have deterministic opaque-byte transcripts,
      embedded self-certifying publisher material, and a verify-before-accept
      API; tampered roots, versions, databases, and signatures cannot advance
      the pure root-chain verifier.
- [x] Rotation-chain and revocation distribution remain explicitly unsupported
      in draft-00. Their message types are reserved, unknown frames are
      rejected before negotiation, and the documentation makes no rotation
      interoperability claim.

## Required before calling wire profile 1 interoperable

- [x] Authority derivation and identity-record transcripts use the draft's
      deterministic-CBOR v1 object. JVM/JS/Python pin the same genesis bytes,
      full Authority ID, routing projection, and record-signing bytes.
- [x] Authentication uses the full 32-byte Authority ID; the compact UUID is
      retained only as Kabel's routing projection.
- [x] Convert gossip signature bytes from host-native `hasch/edn-hash` to the
      deterministic profile-1 envelope and publish a JVM/JS/Python signature
      fixture. The native Kabel adapter remains an explicitly transitional
      compatibility profile.
- [x] Move publication values across the standards boundary as opaque bytes so
      registered Boring tags survive unchanged and relays never reconstruct
      host-language values for signature verification. Strict encode/decode,
      self-verifying envelope, and bounded Python ordered receiver now exist.
- [ ] Route handshake, dissemination, content, and sync runtime messages through
      `netz.wire` rather than native Clojure values. Strict sync-frame conversion
      exists, but is not enabled without capability negotiation.
- [x] Add canonical identity, publication signature and malformed-input
      fixtures.
- [x] Implement cryptographically bound capability/version negotiation in the
      Noise handshake. Runtime codec selection remains the separate item above;
      `wire/netz-cbor/1` is not aliased to the transitional `netz/1` token.
- [ ] Complete a Python identity/handshake/dissemination/sync client. The
      identity, secure carrier, signed publisher, bounded ordered receiver, and
      bounded cumulative-ACK sync sender/atomic receiver exist; subscription
      repair, content exchange and reconnect orchestration remain.
- [x] The optional Python secure-client prototype signs and verifies identity
      records, binds Authority IDs to authenticated Noise statics, enforces the
      expected dial target, fragments bounded inner frames, and passes a real
      asyncio WebSocket round trip plus the same Cacophony vector as Geheimnis.
      Dissemination and outbound sync orchestration remain before calling it a
      complete application client.
- [x] JVM and Python consume the same deterministic direct-binding fixture,
      covering both identity records and binding signatures, the offer, all
      three Noise messages, handshake hash, encrypted serializer-14 fragment,
      and serializer-0 outer carrier. CLJS runs the same binding logic and
      adversarial transport suite; consuming this JSON fixture there remains
      part of the three-runtime gate below.
- [ ] Pass independent JVM/JS/Python two-peer conformance tests.
- [x] Pass a real three-peer offline/rejoin test over http-kit, Kabel CBOR and
      authenticated Noise for both a fenced single-writer root and a
      multiwriter join-semilattice.
- [x] The deterministic production-state-machine simulator passes three-peer
      partition/rejoin scenarios for both profiles, and the secure WebSocket
      test exercises the corresponding runtime layer.

## Artifact commands

```sh
clojure -X:test :dirs '["test"]'
# Exact merged Kabel transport/Python-carrier and unreleased Geheimnis Noise commits:
clojure -X:deps prep :aliases '[:secure-test]'
clojure -X:secure-test :dirs '["test" "test-secure"]'
clojure -X:konserve:test :dirs '["test" "test-konserve"]'
# With ../urd, ../datahike, ../yggdrasil, and ../spindel checked out:
clojure -X:stack :dirs '["test-stack"]'
npm ci
npx shadow-cljs compile :node-test
node target/node-tests.js
python3 -m unittest discover -s interop/python -v
python3 -m pip wheel . --no-deps --wheel-dir /tmp/netz-python-dist
clojure -M:format
clojure -T:build jar
```

Deployment is intentionally not part of CI. `clojure -T:build deploy` requires
explicit Clojars credentials and a human-selected release tag.
