# Changelog

All notable changes to Netz will be documented here. The project follows
Semantic Versioning once the wire profile leaves draft status; alpha releases
may make incompatible protocol changes.

## 0.1.0-alpha.1 — unreleased

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
- Scoped publication sequences and anti-entropy to topic streams, with bounded
  FIFO delivery and synchronization frontiers for recovery.
- Made the deterministic simulator preserve Kabel's per-link FIFO guarantee by
  default while retaining an explicit unordered-transport mode.
- Added shrinkable fault schedules, true crash/durable-restart semantics,
  duplication, in-flight and bandwidth accounting, a 1,000-peer scale gate,
  and a pinned JVM/JavaScript simulation fingerprint.
- Added a pure bounded `sync/open`/`chunk`/`ack`/`done`/`abort` session machine,
  atomic transport-frontier commits, and executable fenced-root and
  join-semilattice reference profiles.
- Added a strict RFC 8949 deterministic-CBOR draft profile, shared JVM/JS
  fixture, and dependency-free Python conformance codec.
- Added a canonical synchronization fixture and a dependency-free Python
  ordered/atomic sync receiver with join-set and fenced-root profiles.
- Added an optional stack gate composing Urd/Raft, Datahike, Netz,
  Spindel, and durable Yggdrasil G-Sets across partition and recovery.
- Replaced ignored internal-design references with public release and
  architecture documentation.
- Added JVM, ClojureScript, Konserve, Python, formatting, and artifact CI gates.
- Corrected the repository license to Apache-2.0.

Known gates before public-network use are tracked in `RELEASE.md`.
