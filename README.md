# netz

A peer-to-peer overlay for [kabel](https://github.com/replikativ/kabel):
self-certifying identity, membership, gossip dissemination with anti-entropy
repair, and content routing — plus a transport that runs `kabel.pubsub` over
the mesh instead of a single connection.

Cross-platform: every protocol namespace is `.cljc` and runs on the JVM and in
the browser.

> ## ⚠️ Alpha. Not safe to expose to untrusted peers.
>
> This is not a caveat about API churn — it is a statement about the current
> code. An adversarial review reproduced, empirically, several ways for **one
> frame from any connected peer** to permanently kill a node's overlay:
>
> Several have since been fixed — a schema gate at the ingress plus a guard on
> both event loops closed the one-frame remote kills, and the identity
> handshake now carries a challenge so a captured hello cannot be replayed.
> What is left:
>
> - repair (`:want`) is a measured ~3000× amplifier: `:max-want` is applied
>   per range rather than per frame;
> - `:seen`, `:store-floor` and `:escalated` are bounded per key but not in
>   the NUMBER of keys, and identities cost one keygen;
> - the address book evicts proven anchors for unproven strangers, and a
>   gossiped entry with no `:group` bypasses diversity entirely;
> - three ring buffers use `subvec`, which retains every element ever held.
>
> Run it between peers you control. Do not put it on a public network yet.
> Hardening these is the current work; see `.internal/` for the review record.

## Why it is a separate library

kabel is a released, stable transport. This is neither. Keeping them apart
means netz can break its API to fix the problems above without a released
library breaking with it — and it keeps kabel's dependency graph free of
crypto, storage and a simulator that only the overlay needs.

kabel exposes exactly one seam for this: a pluggable pub/sub transport.
Everything else here sits on top of kabel's public API.

## What it does

| namespace | |
|---|---|
| `netz.identity` | self-certifying ids — `SHA-256("netz/peer-id/v2" ‖ hasch(genesis))`, the hash of a genesis **record** rather than a key, so rotation does not change identity |
| `netz.membership` | address book, dial policy, backoff, group diversity, peer exchange |
| `netz.dissemination` | flood + interval-set repair, hop TTL, no-echo, interest filtering, per-hop authorization, repair horizon |
| `netz.content` | content routing, subtree transfer, chunking, sealed blocks |
| `netz.pubsub` | `kabel.pubsub` over the overlay: publishes disseminate, the handshake stays point-to-point |
| `netz.overlay.runtime` | the only impure namespace — channels, timers, credentials, the connection registry |
| `netz.sim` | deterministic discrete-event simulator: virtual clock, seeded RNG, total event ordering |

The protocol layers are **pure state machines** of the shape
`(state, event, ctx) -> {:state _ :actions _}`. The runtime is the only thing
that interprets actions. That is what lets the simulator drive the real
protocol code rather than a model of it.

## The idea worth stealing

Moving pub/sub onto a mesh should be a *substitution*, not a rewrite, because
`PSyncStrategy` was never transport-aware. Only two things actually differ:

- **publish** becomes dissemination — multi-hop, signed at origin, verified at
  every hop, deduplicated, repairable;
- **subscribe** becomes topic interest, so a relay can carry one store id
  without carrying the network.

The handshake deliberately stays point-to-point: it is a bulk, acknowledged,
backpressured transfer, and broadcasting one peer's catch-up is the opposite of
what it is for.

## Measured

- flood amplification ≈ average degree, independent of network size
- subtree fetch of a 4 681-node tree: **100 ms flat**, against 29.2 s per-key
- repair asks for exactly the hole (`{:lo 8 :hi 11}`), not the history
- Ed25519 verify 0.60 ms

## Read before deploying to people

kabel's [`doc/MODERATION.md`](https://github.com/replikativ/kabel/blob/main/doc/MODERATION.md)
applies here and is not optional reading. An identity costs one key
generation, so suspension, ban-evasion resistance and deletion are
**structurally impossible**. Owned range → grants → prevention; open range →
labels → advice. If you need more than that, you need a server, and that is a
legitimate answer.

## Tests

```
clojure -X:test :dirs '["test"]'            # 179 tests, 8351 assertions
npx shadow-cljs compile :node-test && node target/node-tests.js
                                            # 165 tests, 8275 assertions
clojure -X:konserve:test :dirs '["test" "test-konserve"]'
```

## License

Copyright © 2026 Christian Weilbach. Distributed under the Apache License 2.0.
