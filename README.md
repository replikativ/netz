# Netz

Netz is an authenticated peer-to-peer networking layer for
[Kabel](https://github.com/replikativ/kabel). It provides peer identity,
membership, topic dissemination, recent-message repair, content exchange, and
an application-defined state-synchronization boundary.

The reference implementation runs on the JVM and in JavaScript through shared
Clojure/ClojureScript sources. A language-neutral protocol draft and a small
dependency-free Python CBOR/session core—including strict signed dissemination
and repair frames—plus a durable filesystem blob repository and transfer
machines define the path to implementations outside the Clojure ecosystem.

> **Pre-release target: `0.1.0-alpha.1` (Apache-2.0).** JVM, CLJS and Python
> share protocol fixtures and live interoperability tests. The experimental
> blob backend also has signed placement, repair and writer-process recovery
> tests; it is not yet a production distributed storage service. See
> [release readiness](RELEASE.md) and the [current storage verification](spec/stable-layout-draft.md)
> for evidence and remaining work.
>
> Public dialing is limited to explicitly configured `wss` gateways. Arbitrary
> public discovery still needs socket-level resolved-IP pinning. Identity-key
> rotation remains reserved.

## What Netz owns

| Layer | Responsibility |
|---|---|
| Identity | Self-certifying peers and connection proof |
| Membership | Bounded peer exchange, dialing, backoff, and topology diversity |
| Dissemination | Signed topic messages, deduplication, and bounded repair |
| Content | Content-addressed block discovery and subtree transfer |
| Durable blobs | Experimental Konserve/file placement, receipts, lifecycle, and repair |
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
clojure -M:test:secure-test:konserve:cljs -m shadow.cljs.devtools.cli compile node-test
node target/node-tests.js
python3 -m unittest discover -s interop/python -v
python3 -m pip wheel . --no-deps --wheel-dir /tmp/netz-python-dist
clojure -T:build jar
```

The complete Python suite needs the optional secure client dependencies first:

```sh
python3 -m pip install '.[secure-client]'
```

The same environment can run the mixed Python/JVM transport proof:

```sh
NETZ_PYTHON=python3 clojure -X:secure-test:mixed-test :dirs '["test-mixed"]'
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

The experimental durable-blob profile uses signed, hash-chained replica layouts
and treats owner and physical site as explicit failure domains. A detected
controller fork stops new retention admission. Its explicit controller runtime
restores durable intents and signed receipts before readiness, then persists
each begin, signed revision, or repair generation before handing offers to the
transfer runtime.
An optional portable scheduler now scans that durable evidence, waits an
initial grace period, and retries known-missing targets with bounded concurrency
and backoff. It does not treat a receipt as proof of future or Byzantine
retention.
The small executable walkthrough models encrypted data held by three private
home devices, quorum at two copies, and repair after one device is lost:

```sh
clojure -M:blob-demo
```

## Documentation

- [Getting started](docs/getting-started.md) — dependencies, lifecycle, and API
- [Architecture](docs/architecture.md) — boundaries and stack integration
- [Synchronization](docs/synchronization.md) — live, repair, and state-sync paths
- [Interoperability](docs/interoperability.md) — AT Protocol, libp2p, Willow, and DHT fit
- [Protocol draft](spec/netz-draft-00.md) — portable API and CBOR wire profile
- [Durable blob design](docs/resilient-blob-store.md) and
  [extension draft](spec/blob-replication-draft-00.md) — optional private replicas
- [Blob backend roadmap](docs/blob-backend-roadmap.md) — storage guarantees,
  stable-placement prototype, migration safety, and remaining release gates
- [Replica connectivity](docs/replica-connectivity.md) — bounded placement-target
  dial priority, lifecycle, diagnostics, and guarantee boundaries
- [Datahike locality experiment](docs/datahike-locality.md) — producer-planned
  durable replicas versus consumer-populated read caches
- [Storage GC](docs/storage-gc.md) — Datahike/Yggdrasil reachability versus
  pool retention and surplus-replica retirement
- [Storage consolidation](docs/storage-consolidation.md) — Konserve checkpoint
  integration, shared validation and remaining backend qualification
- [Konserve-backed object exchange](docs/konserve-object-exchange.md) — stack
  integration, object/ref publication boundary, and Git-like materialization
- [Python interoperability core](interop/python/README.md) — CBOR, sync,
  reconnecting dissemination, and durable-blob APIs
- [Release readiness](RELEASE.md), [security policy](SECURITY.md), and
  [changelog](CHANGELOG.md)
- [Maintainer release procedure](docs/releasing.md) — synchronized JVM/Python
  versions and the explicitly triggered publication pipeline

## License

Copyright © 2026 Christian Weilbach. Licensed under the
[Apache License 2.0](LICENSE).
