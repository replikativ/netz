# Documentation

- [Getting started](getting-started.md) explains the reference implementation's
  dependency, peer construction, startup, pub/sub installation, and shutdown.
- [Architecture](architecture.md) defines Netz's boundary in the wider stack.
- [Synchronization](synchronization.md) separates live dissemination,
  recent-message repair, and application state synchronization.
- [Kabel pub/sub lifecycle design](kabel-pubsub-design.md) defines the
  snapshot/live ordering barrier, compatibility repair, and portable boundary.
- [Deterministic simulator](simulator.md) covers scale, shrinkable schedules,
  faults, durability, and resource modelling.
- [Interoperability](interoperability.md) records what Netz can reuse from
  adjacent protocols and where semantic adapters are required.
- [Resilient blob-store profile](resilient-blob-store.md) proposes optional
  Konserve-backed placement, durability receipts, repair, retention and
  erasure semantics.
- [Durable blob replication extension](../spec/blob-replication-draft-00.md)
  freezes the portable placement subset and records the remaining wire gates.
- [Protocol draft](../spec/netz-draft-00.md) is the prospective language-neutral
  contract. It is not yet an interoperability claim.
- [Release readiness](../RELEASE.md) is the operational and standards gate.
