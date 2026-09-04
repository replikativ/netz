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

> **Release status: `0.1.0-alpha.1`.** The JVM, JavaScript, and Python paths now
> share deterministic CBOR and authenticated Noise/Kabel fixtures. The secure
> JVM overlay passes a real three-peer rejoin test, and a Python sender now
> completes a real authenticated, fsync-backed transfer into the JVM runtime;
> the reverse JVM-provider/Python-receiver path also survives a process restart
> after a durable piece but before its ACK. The JVM runtime also repairs an
> offline signed publication through the negotiated portable dissemination
> carrier over a real Noise/Kabel WebSocket, and the Python subscriber can
> restore application-committed frontiers from an atomic fsynced record and
> run a bounded state-sync pull on that same authenticated connection.
> Exact configured
> `wss` gateways have a supported dial policy. Arbitrary public discovery still
> waits on socket-level resolved-IP pinning. The authenticated transport uses
> released Kabel 0.3.134 and Geheimnis 0.2.39 artifacts. Rotation remains
> explicitly reserved. See
> [release readiness](RELEASE.md) for the exact gates.

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
npx shadow-cljs compile :node-test
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
- [Konserve-backed object exchange](docs/konserve-object-exchange.md) — stack
  integration, object/ref publication boundary, and Git-like materialization
- [Python interoperability core](interop/python/README.md) — CBOR, sync,
  reconnecting dissemination, and durable-blob APIs
- [Release readiness](RELEASE.md), [security policy](SECURITY.md), and
  [changelog](CHANGELOG.md)

## License

Copyright © 2026 Christian Weilbach. Licensed under the
[Apache License 2.0](LICENSE).
