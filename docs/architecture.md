# Architecture

Netz is the reusable network substrate between Kabel transports and replicated
application state. It should remain smaller than the programming model built
on top of it.

```text
Dvergr / Simmis application model and UI bindings
  Spindel signals, Yggdrasil deltas, Datahike roots
    synchronization profile and application authorization
      Netz identity, membership, dissemination, repair, content
        Kabel WebSocket transport
```

## Protocol shape

The protocol namespaces are pure transition functions of the form:

```text
(state, event, context) -> (new state, actions)
```

The runtime interprets actions using channels, timers, Kabel connections, and
optional storage. The deterministic simulator drives the same transition code
as production, which makes partitions, reordering, loss, and repair testable
without wall-clock timing.

## Boundaries

Netz owns:

- transport-peer identity and proof of key possession;
- bounded membership and connection policy;
- topic interest, authenticated dissemination, and recent-message repair;
- content-provider discovery and verified block transfer;
- triggering an application sync when transport repair cannot recover state.

Netz does not own:

- who is allowed to read, publish, write, decrypt, or vote;
- database transaction or CRDT merge semantics;
- Raft elections and committee reconfiguration;
- storage layout or at-rest encryption;
- UI reactivity and application orchestration.

This separation is a security property. Peer exchange is useful discovery but
cannot add a Raft voter, grant a write capability, or confer access to an
encrypted store.

## Stack integration

### Datahike and Urd

The intended single-writer profile carries signed, monotonically fenced root
records. Urd selects and fences the writer through a configured Raft group;
Netz publishes roots and transfers the referenced Datahike/Konserve blocks.
Raft traffic itself needs reliable addressed RPC and must not treat the gossip
mesh as an ordered consensus channel.

### Spindel and Yggdrasil

Spindel's signal synchronization already has the correct semantic boundary: a
summary/handshake transfers current state, while live publications carry
convergent deltas. This should become the first implementation of the draft
`netz/join-semilattice/1` profile. Yggdrasil's join and delta algebra remain
above Netz.

### Dvergr and Simmis

Dvergr can expose the integrated programming model—open a replicated value,
observe it, apply a local change, and inspect sync status—without importing
network state machines. Simmis is the vertical integration target: browser and
server peers, authorization at the application boundary, encrypted Konserve
blocks, and Datahike or Yggdrasil state synchronized through the same Netz
node.

### Replikativ and Datastar lessons

Replikativ's durable lesson is the user-facing `peer`/`stage`/replicated-value
model, not a requirement to put CRDT types back into the network. Datastar's
useful lesson is a small language-neutral core with ergonomic language
bindings and conformance tests. Its server-to-browser SSE authority model is
not the Netz transport model, which must remain bidirectional, offline-capable,
and multiwriter-safe.

## Portable API strategy

Use three layers:

1. a small asynchronous node API shared in meaning across languages;
2. generated message constants, validation models, fixtures, and error codes;
3. handwritten transport and state-machine runtimes for each language.

Schema generation should not generate the concurrency or networking engine.
Independent runtimes should instead prove compatibility against canonical byte
fixtures, malformed corpora, and black-box multi-peer tests.
