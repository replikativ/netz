# Netz

Netz is an authenticated peer-to-peer networking layer for
[Kabel](https://github.com/replikativ/kabel). It provides peer identity,
membership, topic dissemination, recent-message repair, content exchange, and
an application-defined state-synchronization boundary.

The reference implementation runs on the JVM and in JavaScript through shared
Clojure/ClojureScript sources. A language-neutral protocol draft and a small
dependency-free Python CBOR/session core define the path to implementations
outside the Clojure ecosystem.

> **Release status: `0.1.0-alpha.1`.** The JVM, JavaScript, and Python paths now
> share deterministic CBOR and authenticated Noise/Kabel fixtures, and the
> secure JVM overlay passes a real WebSocket test. Public deployment still
> waits on the Kabel/Geheimnis dependency releases, address-resolution policy,
> and the explicitly reserved identity-rotation protocol. See
> [release readiness](RELEASE.md) for the exact gates.

## What Netz owns

| Layer | Responsibility |
|---|---|
| Identity | Self-certifying peers and connection proof |
| Membership | Bounded peer exchange, dialing, backoff, and topology diversity |
| Dissemination | Signed topic messages, deduplication, and bounded repair |
| Content | Content-addressed block discovery and subtree transfer |
| Synchronization | Pure acknowledged sessions with application-defined profiles |
| Runtime | Kabel/WebSocket integration, limits, lifecycle, and persistence hooks |

Discovery is not authority. Netz may find a peer or content provider, but
application policy decides who may publish, write, decrypt, or vote. Raft
membership, CRDT semantics, access control, and storage encryption remain
above the network layer.

## Start here

Build and test the current checkout:

```sh
clojure -X:test :dirs '["test"]'
npm ci
npx shadow-cljs compile :node-test
node target/node-tests.js
python3 -m unittest discover -s interop/python -v
python3 -m pip wheel . --no-deps --wheel-dir /tmp/netz-python-dist
clojure -T:build jar
```

Maintainers with sibling checkouts can also run the real stack seams without
adding them to the published artifact:

```sh
clojure -X:stack :dirs '["test-stack"]'
```

The curated application API is `netz.core`. Constructing the underlying Kabel
peer is transport-specific, so the complete lifecycle is documented in
[Getting started](docs/getting-started.md).

## Design

Netz combines two paths:

- live publications travel through the overlay and can be repaired while they
  remain inside the bounded repair horizon;
- initial joins, old gaps, and local delivery overflow use a point-to-point,
  application-defined state sync.

This keeps database and CRDT semantics out of the network. Kabel's
`PSyncStrategy` is the current Clojure/JVM/JS adapter. The draft portable API
generalizes the same contract to asynchronous language bindings.

A Kademlia DHT is deliberately not required. Bounded peer exchange and
two-hop provider discovery cover the current deployments; a DHT can later be
added as another routing provider without changing application sync.

## Documentation

- [Getting started](docs/getting-started.md) — dependencies, lifecycle, and API
- [Architecture](docs/architecture.md) — boundaries and stack integration
- [Synchronization](docs/synchronization.md) — live, repair, and state-sync paths
- [Interoperability](docs/interoperability.md) — AT Protocol, libp2p, Willow, and DHT fit
- [Protocol draft](spec/netz-draft-00.md) — portable API and CBOR wire profile
- [Python interoperability core](interop/python/README.md) — CBOR and sync seed
- [Release readiness](RELEASE.md), [security policy](SECURITY.md), and
  [changelog](CHANGELOG.md)

## License

Copyright © 2026 Christian Weilbach. Licensed under the
[Apache License 2.0](LICENSE).
