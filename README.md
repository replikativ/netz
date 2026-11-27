# Netz

[![Clojars version](https://img.shields.io/clojars/v/org.replikativ/netz.svg)](https://clojars.org/org.replikativ/netz)
[![PyPI version](https://img.shields.io/pypi/v/netz-protocol.svg)](https://pypi.org/project/netz-protocol/)
[![CircleCI build on main](https://img.shields.io/circleci/build/github/replikativ/netz/main.svg)](https://circleci.com/gh/replikativ/netz/tree/main)
[![Last commit on main](https://img.shields.io/github/last-commit/replikativ/netz/main.svg)](https://github.com/replikativ/netz/commits/main/)

Netz is an authenticated peer-to-peer networking layer for
[Kabel](https://github.com/replikativ/kabel). It provides peer identity,
membership, topic dissemination, recent-message repair, content exchange, and
an application-defined state-synchronization boundary.

The reference implementation runs on the JVM and in JavaScript through shared
Clojure/ClojureScript sources. A language-neutral protocol draft and a small
dependency-free Python CBOR/session core—including strict signed dissemination
and repair frames—plus a durable filesystem blob repository and transfer
machines define the path to implementations outside the Clojure ecosystem.

> **Alpha series: `0.1.<commit-count>` (Apache-2.0).** JVM, CLJS and Python
> share protocol fixtures and live interoperability tests. The experimental
> blob backend also has signed placement, repair and writer-process recovery
> tests; it is not yet a production distributed storage service. See
> [release readiness](RELEASE.md) and the [current storage verification](spec/stable-layout-draft.md)
> for evidence and remaining work.
>
> Public dialing is limited to explicitly configured `wss` gateways. Arbitrary
> public discovery still needs socket-level resolved-IP pinning. Identity-key
> rotation remains reserved.

## Use it from an application

Suppose a writer commits a new database snapshot while a reader is subscribed to
its branch. The writer announces a small signed reference on a topic. The reader
verifies the publisher, fetches the catalogue and any missing blocks by content
ID, and opens that exact commit. Later readers can catch up from the writer's
stored references, even if they missed the live announcement.

The writer and reader are ordinary Kabel peers. In this two-peer JVM example,
the writer listens on a loopback WebSocket and the reader dials it using the
writer's routing ID, Authority ID and address. The example's `start-node!`
helper calls Kabel's `server-peer` or `client-peer`, starts an authenticated
Netz overlay, installs Netz pub/sub, and returns both the Kabel peer (`:peer`)
and the Netz runtime (`:running`). [Peer setup](examples/netz/examples/datahike_replica.clj)
· [Building a peer in your own application](docs/getting-started.md#authenticated-transport)

```clojure
(require '[kabel.pubsub :as kabel]
         '[netz.core :as netz]
         '[netz.examples.datahike-replica :as demo]
         '[netz.identity :as id]
         '[netz.overlay.runtime :as runtime]
         '[netz.snapshot-ref :as ref])

(def writer-keys (demo/await! (netz/generate-identity)))
(def writer-authority (id/authority-id (:genesis writer-keys)))
(def url (demo/free-url))
(def writer-node (demo/start-node! writer-keys url nil))
(def seed {:peer-id (:id writer-node) :authority-id writer-authority
           :addresses [url] :group "seed"})
(def reader-node (demo/start-node! (demo/await! (netz/generate-identity)) nil seed))

(def writer (:peer writer-node))
(def reader (:peer reader-node))
(demo/wait-for! #(contains? (runtime/connections reader) (:id writer-node)))
```

Once connected, register a snapshot-reference strategy at each end. Here the
writer announces a single example revision and the reader waits until it has
verified and accepted that revision.

```clojure
(def stream {:profile "demo/revision-v1" :system (id/utf8-bytes "notes")
             :ref (id/utf8-bytes "main")})
(def writer-state (atom (ref/make-state)))
(def reader-state (atom (ref/make-state)))
(defn strategy [state]
  (netz/snapshot-ref-strategy (assoc stream :state state :publisher writer-authority)))

(try
  (kabel/register-topic! writer demo/topic {:strategy (strategy writer-state)})
  (demo/await! (netz/subscribe! reader #{demo/topic}
                                {:strategies {demo/topic (strategy reader-state)}}))

  (let [revision (demo/await! (ref/sign writer-keys
                                (assoc stream :snapshot (id/utf8-bytes "rev-1") :parents [])))
        [history outcome] (demo/await! (ref/accept-verified @writer-state revision))]
    (assert (= :accepted outcome))
    (reset! writer-state history) ; make the revision available for later catch-up
    (let [result (demo/await! (netz/publish! writer demo/topic (ref/encode revision)))]
      (assert (:ok result)))
    (demo/wait-for! #(contains? (ref/heads @reader-state (:profile stream)
                                           (:system stream) (:ref stream))
                                (ref/snapshot-key revision))))
  (finally
    (demo/stop-node! reader-node)
    (demo/stop-node! writer-node)))
```

Run `clojure -M:replica-demo` for the complete example, including identities,
strategy construction and block verification. The reader starts with 1,200
notes, sees one live addition, catches up two missed additions, and queries all
1,203 from disk after the writer goes away. [Runnable source](examples/netz/examples/datahike_replica.clj)
· [Walkthrough](docs/datahike-replica-demo.md)

## How state reaches another peer

A publication has an origin, epoch, topic and sequence number. Consider a reader
that has applied through sequence 6 in one stream and receives 8 before 7.

```mermaid
sequenceDiagram
    participant W as Writer
    participant P as Subscribed peer with history
    participant R as Reader
    W->>P: Signed publication 8
    P->>R: Forward publication 8
    Note over R: Verify and buffer 8 while 7 is missing
    P->>R: Advertise sequence ranges and repair horizon
    alt Peer still retains 7
        R->>P: Request 7
        P->>R: Signed publication 7
        Note over R: Apply 7, then drain 8
    else Gap is older than retained history
        R->>P: Begin state sync with an authorized provider
        P->>R: Profile-specific catch-up data
        Note over R: Commit locally, then drain the uncovered suffix in order
    end
```

The diagram uses one neighbor that can both relay and serve state. A deployment
can assign those roles to different peers. Successful `publish!` means local
acceptance. The reader advances its application frontier only after its own
apply succeeds. Ordering is per origin, epoch and topic; an application with
multiple writers defines how their updates combine.

For example, the reference grow-only set profile makes that last choice explicit.
If the writer holds `{a, b, c}` and the reader holds `{b, d}`, the reader can
catch up while preserving its own addition `d`:

```text
reader summary   = {b, d}
writer delta     = writer state ∖ reader summary = {a, c}
staged state     = reader state ∪ received chunks
committed state  = current reader state ∪ staged state ∪ writer checkpoint
                 = {a, b, c, d}
```

Set union makes repeated chunks harmless and independent additions commute.
Other profiles use different rules. The database example verifies a signed
snapshot reference and its blocks. Netz supplies peer authentication, bounded
repair and the sync session; the profile decides which state to accept.
[Set profile](src/netz/sync/join_semilattice.cljc) · [Synchronization model](docs/synchronization.md)

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

Compile the three Node peers before running the mixed JVM, Node, and Python tests:

```sh
clojure -M:test:secure-test:konserve:mixed-test:cljs -m shadow.cljs.devtools.cli compile mixed-content mixed-sync mixed-dissemination
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
- [Multi-process Datahike experiment](docs/datahike-process-experiment.md) —
  static EDN bootstrap, cross-process `N/W` durability, replacement repair and
  an offline query after writer loss
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
  commit-count versions and automatic publication from green `main`

## License

Copyright © 2026 Christian Weilbach. Licensed under the
[Apache License 2.0](LICENSE).
