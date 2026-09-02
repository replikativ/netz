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
- [x] JVM, browser/Node, optional store, Python fixture, mixed JVM/Python, and
      jar build are CI gates.
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

- [x] Bind the identity proof to a cryptographic transport/session and
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
      Kabel 0.3.126 contains the required transport context and Python carrier;
      Geheimnis 0.2.39 contains the Noise binding.
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

## Experimental durable-blob extension

This extension is not part of the alpha interoperability claim yet.

- [x] Separate immutable blob durability from `konserve-sync` logical-key and
      database synchronization.
- [x] Define deterministic rendezvous placement over verified full Authority
      IDs with explicit owner/site constraints and shared Clojure/Python
      fixtures.
- [x] Enforce a durable-outbox transition before network stores, fence retries
      by attempt, and simulate crash recovery, partitions, disk loss, full and
      failed disks, corruption notification, and repair.
- [x] Freeze portable object, piece and transfer-manifest addresses plus the
      bounded resume bitmap in JVM/JavaScript/Python fixtures.
- [x] Provide an explicit optional Konserve immutable-write seam and a runnable
      three-private-device demonstration without claiming a transparent store.
- [x] Freeze authenticated offer/piece, retention-manifest, signed storage
      receipt, lease, deletion-marker and deletion-receipt schemas plus a
      JVM/JavaScript/Python-compatible deterministic fixture. The capability
      remains unadvertised until the runtime and lifecycle gates below pass.
- [x] Implement a bounded pure receiver that resumes verified staging, applies
      one-piece persistence backpressure, ACKs only staged pieces, requires an
      accepted install durability level, and signs receipts only afterwards.
- [x] Implement a bounded pure sender that retains no piece bytes in control
      state, obeys one-piece receiver backpressure, fences authenticated
      receipts by session/layout/attempt, and durably records a verified signed
      receipt before releasing it to quorum accounting.
- [x] Define the portable asynchronous transfer-store API and implement the JVM
      reference file store with independently verified content-addressed
      staging, atomic/fsynced installation markers, bounded cryptographically
      verified receipt replay, and restart-resume integration tests.
- [x] Implement a dependency-free asynchronous Python filesystem repository
      with JVM-compatible piece paths and deterministic installation markers,
      explicit fsync/best-effort claims, restart/resume, bounded inventory,
      scrubbing and bounded reads.
- [x] Implement independent one-session Python sender and receiver machines.
      They expose typed carrier/storage/signing actions, preserve one-piece
      staging backpressure and authenticated session/attempt fences, and gate
      controller delivery on durable receipt persistence. JVM, JavaScript and
      Python pin the same need/piece/ACK/commit trace bytes. A reconnecting
      general-purpose Python carrier runtime remains a separate gate.
- [x] Implement a journal-first lifecycle machine with monotone leases,
      same-revision equivocation evidence, expiry, deletion retry, absorbing
      tombstones, and persist-before-emit deletion receipts. Simulator tests
      cover crash recovery and bulk expiry without sockets.
- [x] Implement the JVM Kabel/Noise transfer runtime through a generic
      capability-gated overlay extension dispatcher. Real-carrier tests cover
      explicit negotiation, multi-piece fsync installation, policy rejection,
      invalid-signature disconnect, receipt persistence and unsupported-peer
      refusal. The capability remains absent from Noise defaults.
- [x] Bind named extensions to the language-neutral wire profile with the
      separately negotiated `netz/extension-cbor/1` carrier and a closed
      deterministic inner envelope. JVM/JavaScript/Python pin identical
      preferred bytes; negotiated JVM transfers use the portable path while
      older Clojure peers retain an explicit transitional fallback. Python
      enforces the transcript capability set and exposes bounded carrier codecs
      for its secure WebSocket client, but not yet the reconnecting runtime.
- [x] Run a real Python sender against the JVM/http-kit receiver: Noise XX
      authenticates both Authority IDs, the portable carrier moves the complete
      piece/ACK/commit flow, the JVM fsyncs and signs its receipt, and Python
      verifies and fsyncs that receipt before declaring completion. This is a
      dedicated CI job with independently provisioned Python dependencies.
- [x] Implement a portable production lifecycle-policy adapter which restores
      before authorization, journals the exact retention statement before byte
      transfer, verifies reconciled lease/deletion envelopes, and fails closed
      when backend erasure is not explicitly implemented. JVM and Node
      Konserve memory backends cover restart ordering.
- [x] Implement bounded, sorted local installed-manifest inventory and
      full-object scrubbing for the JVM file and portable Konserve stores.
      JVM corruption tests and Node Konserve tests share the same API.
- [x] Implement authenticated provider retrieval with separate read and offer
      authorization, exact pending-request fencing, bounded timeout/capacity,
      and reuse of the receipt-producing transfer path. JVM and Python pin the
      fetch frame, JavaScript runs the same fixture, and a real Kabel/Noise test
      covers success and policy denial.
- [x] Add a result-bearing fetch API and portable, bounded local-first
      `ensure-local!`: scrub local storage, try only caller-selected
      authenticated providers, scrub after install, and report explicit local,
      fetched or unavailable outcomes. JVM/JavaScript cover retry and
      fail-closed post-install validation.
- [x] Add the closed application-object binding and explicit bounded
      materializer. The binding matches an authenticated catalog key/codec to
      the exact retained object and manifest; JVM/JavaScript/Python share its
      deterministic-CBOR and domain-separated-address fixture.
- [x] Freeze a closed signed replica-layout statement and domain-separated
      address, exact hash-chained epoch reconciliation, bounded fork evidence,
      and fail-closed deterministic-target admission. JVM/JavaScript/Python
      share the layout/address/signature vector. The lifecycle runtime exposes
      a migration-safe synchronous layout-state fence.
- [x] Persist and restore the bounded, ordered signed-layout transcript before
      it authorizes an epoch. The serialized runtime re-verifies every envelope
      and predecessor on startup, persists before activation, and fails startup
      closed on corruption; the portable Konserve adapter passes JVM/Node
      restart tests.
- [x] Define signed controller-fork recovery as a terminal rollover to a fresh
      cluster identity. The procedure preserves the old transcript and
      lifecycle obligations, requires out-of-band authorization and fresh
      retention evidence, and never chooses a branch by arrival order or LWW.
- [x] Choose authenticated targeted re-offer as the draft remote repair/audit
      primitive. Local inventory is not exposed over the network; broad or
      Merkle inventory is a later optimization requiring explicit privacy
      scope, authorization, freshness and resource bounds.
- [x] Persist retry generations before re-offer. Per-target attempts and the
      intent revision cross the durable outbox before network actions; stale
      persistence completions and old receipts cannot satisfy a new repair.
- [x] Define a closed portable controller-intent record and asynchronous store
      contract. Restore re-verifies signed layout/retention, addresses, policy
      and exact targets; the Konserve implementation has bounded JVM/Node
      restart and replacement coverage.
- [x] Implement the serialized explicit controller runtime. Startup restores
      intents before receipts and readiness; begin and repair persist before
      dispatch, failed writes retry the identical generation, and the transfer
      runtime can delegate receipt replay to this single owner. The JVM file
      repository supplies the default atomic/fsync controller store, while
      Konserve is an explicitly accepted best-effort option.
- [x] Implement crash-safe signed intent revision for repairable retention and
      non-forked layout/policy migration. The immutable manifest and controller
      stream remain fixed, prior receipts are invalidated, target attempts are
      fenced, and the complete replacement persists before re-offer. Compact
      leases remain limited to already admitted replicas.
- [x] Implement portable bounded scheduling for controller-known missing
      replicas. It waits for controller recovery, protects initial transfers
      with a grace interval, globally bounds outstanding controller repair
      calls, backs off retries, and fences late results by the exact intent
      generation. Transfer-runtime session bounds remain independently active.
- [ ] Implement Node/browser durable storage, backend erasure, and operational
      offline-peer reconciliation.
- [ ] Implement and validate any later transparent Konserve wrapper semantics
      for binary streams, multi-key root-last batches, nested updates and read
      repair.
- [x] Decide the initial Konserve integration boundary: explicit immutable
      object exchange first, with transport pieces hidden below application
      keys and root publication; write-hook, tiered and transparent wrappers
      remain later convenience layers.
- [ ] Pass real three-device JVM/JavaScript/Python conformance and operational
      lease/deletion tests before advertising the capability.

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
      exists, but is not enabled without capability negotiation. Named extension
      traffic now uses the portable carrier when both the wire profile and its
      dedicated carrier capability are selected; this does not overstate the
      remaining base-overlay work.
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
- [ ] Pass independent JVM/JS/Python two-peer conformance tests. The durable
      blob subset now has a real Python-to-JVM two-peer test; base dissemination,
      sync, the reverse blob direction, and a JavaScript peer remain.
- [x] Pass a real three-peer offline/rejoin test over http-kit, Kabel CBOR and
      authenticated Noise for both a fenced single-writer root and a
      multiwriter join-semilattice.
- [x] The deterministic production-state-machine simulator passes three-peer
      partition/rejoin scenarios for both profiles, and the secure WebSocket
      test exercises the corresponding runtime layer.

## Artifact commands

```sh
clojure -X:test :dirs '["test"]'
# Released Kabel 0.3.126 and Geheimnis 0.2.39:
clojure -X:secure-test :dirs '["test" "test-secure"]'
# After installing `.[secure-client]` into the selected interpreter:
NETZ_PYTHON=python3 clojure -X:secure-test:mixed-test :dirs '["test-mixed"]'
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
