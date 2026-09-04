# Changelog

All notable changes to Netz will be documented here. The project follows
Semantic Versioning once the wire profile leaves draft status; alpha releases
may make incompatible protocol changes.

## 0.1.0-alpha.1 — unreleased

- Defined the portable `netz/dissemination/1` interests, anti-entropy and repair
  frames. Strict Clojure/ClojureScript and Python codecs share deterministic
  CBOR fixtures, use full Authority IDs for stream identity, retain payloads as
  opaque signed bytes, and reject unknown, unsorted, duplicate or unbounded
  control data. Profile 1 exposes exact text-topic carrying plus `relay_all`;
  native Clojure vector-prefix ranges remain outside the standard boundary.
  The runtime now selects this grammar only after exact wire/carrier/profile
  negotiation, maps full Authorities to compact routing IDs with collision
  rejection, refuses native downgrade and reserved-carrier smuggling, and
  applies bounded fail-closed boolean/channel policy in both directions. A real
  Noise/Kabel test repairs a publication created while its subscriber was
  offline. Kabel 0.3.134 also closes the concurrent unsubscribe/resubscribe
  generation race exercised by Netz's managed pub/sub lifecycle.
  The Python package now composes these pieces into a bounded dissemination
  session and reconnecting asyncio client with mandatory fail-closed policy,
  periodic `have`, bounded repair serving, ordered delivery and explicit state
  sync escalation. A real Python/JVM test repairs an offline signed root. The
  JVM treats a negotiated portable dissemination connection as a dissemination
  peer without implicitly exposing native membership or content protocols.
  Python delivery acknowledgement is now application-explicit and backed by a
  pluggable frontier store. The single-owner file reference atomically replaces
  fsynced deterministic-CBOR records, restores before dialing, fails closed on
  corrupt records, and never advances memory when persistence fails. Restart,
  contiguous-commit, mutation, regression and stale-queue tests pin its
  at-least-once semantics.
  The same reconnecting client now multiplexes bounded outbound
  `netz/sync/1` pulls on its authenticated connection, applies mandatory
  peer/topic/profile policy to every frame, aborts timed-out sessions, and
  persists only locally committed profile frontiers before returning repaired
  state. The prior one-shot JVM-to-Python conformance peer now uses this API.
  Fixed `SyncSource` routes now provide the inverse reusable serving path:
  authorization precedes source lookup and snapshot IO, concurrent sessions
  and completion queues are bounded, unacknowledged sessions expire, and the
  JVM-to-Python reverse conformance peer uses the same runtime.

- Added a separately negotiated portable `netz/sync/1` carrier path and real
  bidirectional Python/JVM synchronization tests. Portable bytes16 sessions
  are now keyed by tagged content rather than JVM array identity, and malformed
  negotiated sync payloads close the connection. Portable reads are
  deny-by-default behind authenticated Authority/topic/profile authorization;
  each session pins its negotiated carrier and rejects native schema downgrade
  or reconnect-time reinterpretation.

- Added the language-neutral `netz/content/1` single-block profile using
  CIDv1/raw/SHA-256, with strict Clojure/ClojureScript and Python codecs and
  fixtures. Authorization-first services and bounded correlated requesters pass
  real authenticated Python/JVM fetches in both directions. Provider hints now
  live in an expiring cache bounded per CID, per provider, and globally;
  resilient fetches randomize and try only a configured candidate count.
  Concurrent requests atomically reserve their outstanding slots, and repeated
  request-ID collisions fail boundedly.

- Added an experimental private durable-blob extension: deterministic
  Authority-ID placement across owner/site failure domains, explicit durable
  outbox and receipt fencing, portable piece manifests/resume bitmaps with
  JVM/JavaScript/Python fixtures, signed retention/lease/deletion and replica
  receipt schemas, bounded transfer frames, persistence-backpressured pure
  sender/receiver machines with persist-before-quorum signed evidence, a
  portable transfer-store contract, a JVM atomic/fsynced file reference with
  restart receipt verification, a portable Konserve transfer repository with
  JVM/Node restart and install coverage, a capability-gated Kabel/Noise runtime
  with an explicit durable authorization boundary, a portable production
  lifecycle adapter with restore-before-authorize ordering and signed
  lease/deletion ingestion, a journal-first expiry/deletion machine, simulated
  disk/partition/crash repair, an explicit optional Konserve object seam, and a
  three-device demonstration. JVM file and portable Konserve stores also expose
  bounded sorted installed-manifest inventory and full-address object scrubs.
  The Python package now includes an asynchronous filesystem repository with
  the same piece paths and deterministic installation-marker bytes as the JVM,
  plus fsync, restart/resume, inventory, scrub and bounded-read coverage.
  Authenticated providers can now serve an explicitly requested installed
  manifest through the same offer/piece/receipt path, with independent read
  authorization and exact pending-request fencing. A portable bounded
  `ensure-local!` helper now scrubs local storage, retries caller-selected
  providers and requires a clean post-install scrub before reporting success.
  A closed application-object binding and bounded explicit materializer now
  connect authenticated catalog keys/codecs to verified local object bytes;
  JVM, JavaScript and Python share its deterministic-CBOR fixture.
  Signed replica layouts now have a closed portable statement, a
  domain-separated address and an exact predecessor chain. Reconciliation
  rejects gaps and bad links, preserves bounded same-epoch fork evidence, and
  freezes deterministic-target admission after a fork; JVM, JavaScript and
  Python share the layout/address/signature vector.
  A serialized layout runtime now re-verifies and replays a bounded ordered
  envelope transcript before readiness, persists each accepted change before
  activation, and restores both the chain and fork freeze through the portable
  Konserve adapter on JVM and JavaScript. The draft defines a detected signed
  controller fork as terminal for that cluster identity and specifies an
  audited rollover which preserves old lifecycle obligations and requires new
  retention evidence. Draft remote repair and audit now use authenticated
  targeted re-offers: authorized receivers repair missing pieces or re-verify
  an existing installation before issuing a freshly fenced receipt. Local
  inventory is not a network disclosure surface; any later batched or Merkle
  inventory requires explicit privacy scope and resource bounds. Retry attempts
  now cross a revision-fenced durable-outbox action before any re-offer, so a
  crash cannot reset the attempt fence or release work through a stale write
  completion. The pure controller now binds layout and policy per intent and
  restores verified attempts without emitting network work, allowing durable
  receipt replay to precede restart repair. A closed local controller-intent
  record now carries the signed layout/retention, manifest, exact target
  attempts and revision across a bounded portable Konserve restart on JVM and
  JavaScript. A serialized controller runtime now restores intents before
  receipts, persists begin and explicit-repair generations before dispatch,
  retries failed writes without advancing their fence, and rejects stale
  layouts or mismatched signed receipt fields. Transfer receipt replay has a
  single-owner switch for this composition. The JVM file repository now also
  stores controller intents with bounded atomic/fsynced replacement. A real
  Noise/Kabel test covers the complete controller-outbox, transfer, durable
  receipt and quorum-callback path. A portable known-missing repair scheduler
  now waits for controller recovery, protects attempt-zero work with an initial
  grace interval, globally bounds outstanding controller calls, applies a
  bounded backoff ladder, and ignores late results from replaced intent
  generations. Its pure 1,000-object test exercises bounded deterministic
  scheduling without starting additional peers or JVMs. Signed full-retention
  successors can now renew a repairable object, change placement policy, or
  migrate it to a later reconciled layout. The controller atomically replaces
  the durable intent before re-offer, clears prior receipt authority, and
  fences retained and newly added targets. Replica lifecycle accepts monotone
  full-retention/layout advances and later-layout global deletion, while
  control-stream retention is refused on members outside current placement;
  compact leases remain scoped to already admitted replicas.
  The Python package now includes transport-independent one-session durable
  blob sender and receiver machines with typed effects, one-piece persistence
  backpressure, authenticated session fencing, and receipt-persistence gating.
  A dedicated mixed-runtime CI test now drives the Python sender across an
  authenticated Noise/Kabel WebSocket into the JVM fsync repository, verifies
  the JVM-signed receipt in Python, and fsyncs it before sender completion.
  The reverse fetch path serves an installed JVM object into Python's fsync
  repository, returns a Python-signed receipt to the JVM journal, and reopens
  the Python-written repository through the JVM adapter. The reverse test
  fsyncs its exact admitted retention intent before requesting bytes, while
  the provider independently pins the authorized requester and object scope.
  It also interrupts a process after a durable piece but before ACK, then
  reconnects with a fresh session/attempt and transfers only the missing tail.
  JVM, JavaScript and Python pin the same canonical need/piece/ACK/commit trace.
  The explicit Konserve immutable-write seam now fails before network work when
  its local commit fails and delivers thrown replication errors to callers.
  Privacy deletion fails closed unless a backend provides complete erasure.
  The remaining storage and network bindings stay gated in `RELEASE.md`.
- Added a negotiated portable extension carrier with a deterministic closed
  text-key envelope, exact capability binding, fail-closed dispatch, shared
  JVM/JavaScript/Python fixture bytes, Python encode/decode APIs, and an
  explicit native fallback for transitional Clojure-only connections.
- Added the exact `Noise_XX_25519_AESGCM_SHA256` direct Kabel carrier with
  full-Authority target binding, transcript-bound capability negotiation,
  authenticated serializer-14 envelopes, bounded fragmentation, and shared
  JVM/JavaScript/Python fixtures.
- Replaced the interoperable gossip transcript with deterministic profile-1
  CBOR over exact opaque payload bytes, including a shared Ed25519 publication
  fixture and strict self-verifying envelope codecs.
- Added Python signed publication creation/verification and a bounded ordered
  receiver with duplicate suppression, gap reporting, and state-sync frontier
  recovery.
- Added the Python synchronization sender, including exact open/ACK schemas,
  bounded cumulative-ACK flow control, and sender/receiver round-trip tests;
  the JVM sender now rejects ACKs for chunks it has not sent.
- Added a real three-peer secure WebSocket offline/rejoin test that converges
  both fenced-root and multiwriter join-semilattice profiles.
- Added an exact configured-`wss` public-gateway dial policy; dynamic dialing of
  untrusted hostnames remains unsupported until resolved IPs can be validated
  and pinned by the socket adapter.
- Added explicit runtime shutdown and adapter cleanup.
- Separated hot identity persistence from cold rotation/revocation material.
- Added strict known-message schemas and decoded-value resource bounds.
- Added bounded identity-handshake attempts, deadlines and pre-auth buffering.
- Serialized `hello` before `proof` on the ordered Kabel transport.
- Added subtree transfer deadlines and alternate-provider retry.
- Added a configurable WebSocket dial policy.
- Converted application delivery overflow into differential state recovery.
- Unified Kabel snapshot/direct and Netz overlay application under Kabel's
  peer/topic serializer, and rejected identity-less direct live frames while
  the overlay owns a topic.
- Made horizon recovery drain the old Kabel v0 generation before subscribing
  again, serialize/coalesce recovery attempts under one awaited per-topic
  generation through actual snapshot drain, retain candidate-source ownership,
  generation-fence buffered live values, expose terminal failure, and stop
  applying later deltas while desynchronized. Subscribe, unsubscribe, recovery,
  interest rollback, and shutdown now share one explicit lifecycle contract.
- Made ordinary `kabel.pubsub/unsubscribe!` dispatch through Netz's lifecycle
  owner, while recovery uses Kabel's explicit direct drain primitive.
- Added a portable pure profile-apply owner with transport exclusivity,
  item/byte and duplicate-receipt limits, exact apply receipts, snapshot/live
  serialization, generation- and owner-minted sync-attempt fencing, portable
  exact-integer/frontier bounds, absorbing terminal states, and locally bound
  application-committed frontiers.
- Integrated that apply owner into every Netz-managed Kabel-v0 topic. Initial
  and recovery snapshots now gate live overlay application; slow applies,
  owner-minted snapshot attempts, suffix drain, receipt settlement,
  unsubscribe, and shutdown share one ordered generation boundary.
- Preserved authenticated publication identity through application delivery
  and advanced synchronization frontiers only after exact local apply success.
  Legacy sync profiles now claim no live-stream frontier; an opted-in profile
  must validate and mint its own commit receipt.
- Consumed Kabel's exact decoded CBOR frame size as receiver-local accounting
  metadata without relaying that unsigned value.
- Scoped publication sequences and anti-entropy to topic streams, with bounded
  FIFO delivery and synchronization frontiers for recovery.
- Made the deterministic simulator preserve Kabel's per-link FIFO guarantee by
  default while retaining an explicit unordered-transport mode.
- Added shrinkable fault schedules, true crash/durable-restart semantics,
  duplication, in-flight and bandwidth accounting, a 1,000-peer scale gate,
  and a pinned JVM/JavaScript simulation fingerprint.
- Added a pure bounded `sync/open`/`chunk`/`ack`/`done`/`abort` session machine,
  atomic locally validated frontier commits, and executable fenced-root and
  join-semilattice reference profiles.
- Added a strict RFC 8949 deterministic-CBOR draft profile, shared JVM/JS/Python
  `hello`, `sync/open`, and frontier-bearing `sync/done` fixtures, and a
  dependency-free Python conformance codec.
- Added a canonical synchronization fixture and a dependency-free Python
  ordered/atomic sync receiver with join-set and fenced-root profiles.
- Added strict Clojure profile-1 sync-frame conversion, pre-profile validation
  of sender frontier claims, and local canonical receipt-size accounting.
- Added an installable, typed, dependency-free `netz-protocol` Python alpha
  wheel with Apache-2.0 package metadata and a CI build gate.
- Updated to Kabel 0.3.124 for bounded WebSocket frames and transport queues,
  backed by the released bounded replikativ http-kit adapter.
- Added an optional stack gate composing Urd/Raft, Datahike, Netz,
  Spindel, and durable Yggdrasil G-Sets across partition and recovery.
- Replaced ignored internal-design references with public release and
  architecture documentation.
- Added JVM, ClojureScript, Konserve, Python, formatting, and artifact CI gates.
- Corrected the repository license to Apache-2.0.

Known gates before public-network use are tracked in `RELEASE.md`.
