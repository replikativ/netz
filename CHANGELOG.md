# Changelog

All notable changes to Netz will be documented here. The project follows
Semantic Versioning once the wire profile leaves draft status; alpha releases
may make incompatible protocol changes.

## 0.1.0-alpha.1 — unreleased

- Added an experimental private durable-blob extension: deterministic
  Authority-ID placement across owner/site failure domains, explicit durable
  outbox and receipt fencing, portable piece manifests/resume bitmaps with
  JVM/JavaScript/Python fixtures, signed retention/lease/deletion and replica
  receipt schemas, bounded transfer frames, a persistence-backpressured pure
  receiver, a journal-first expiry/deletion machine, simulated
  disk/partition/crash repair, an explicit optional Konserve seam, and a
  three-device demonstration. The real sender, storage and network bindings
  remain gated in `RELEASE.md`.
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
