# Getting started

Netz `0.1.0-alpha.1` targets controlled peer groups. Do not expose it to
untrusted Internet peers until the public-network items in
[`RELEASE.md`](../RELEASE.md) are complete.

## Dependency

To install the current checkout into the local Maven repository:

```sh
clojure -T:build install
```

Then add the following coordinate to a Clojure project:

```clojure
io.replikativ/netz {:mvn/version "0.1.0-alpha.1"}
```

The artifact contains the shared JVM/ClojureScript sources. JavaScript builds
also need the normal ClojureScript toolchain used by the host application.

## Lifecycle

The public facade is `netz.core`, but Kabel still owns socket construction.
Startup therefore has four explicit steps:

1. generate or load an identity;
2. create a deferred Netz middleware and construct the Kabel peer with it;
3. start Netz, then install the returned middleware;
4. start or connect the Kabel transport and install overlay pub/sub.

The middleware indirection is important for Kabel servers because their accept
loop captures middleware at construction time.

This JVM server skeleton shows the boundary:

```clojure
(require '[kabel.http-kit :as http-kit]
         '[kabel.peer :as peer]
         '[netz.core :as netz]
         '[netz.identity :as id]
         '[netz.overlay.runtime :as runtime]
         '[superv.async :refer [<?? S]])

(let [url "ws://localhost:48301"
      keys (<?? S (netz/generate-identity))
      peer-id (id/peer-id (:genesis keys))
      [middleware install!] (runtime/deferred-middleware)
      socket-peer (peer/server-peer
                   S
                   (http-kit/create-http-kit-handler! S url peer-id)
                   peer-id
                   middleware
                   identity)
      node (<?? S (netz/start! S socket-peer
                              {:identity keys
                               :addresses [url]
                               :topics #{:example}}))]
  (install! (:middleware node))
  (<?? S (peer/start socket-peer))
  (netz/install-pubsub! S socket-peer node)

  ;; Subscribe with the same Kabel PSyncStrategy used on the direct transport:
  ;; (<?? S (netz/subscribe! socket-peer #{:example}
  ;;                         {:strategies {:example strategy}}))
  ;; (<?? S (netz/publish! socket-peer :example {:hello "world"}))

  ;; Netz and the owning Kabel listener have separate lifecycles.
  (netz/stop! node)
  (<?? S (peer/stop socket-peer)))
```

For a client, construct `peer/client-peer`, pass a configured seed to
`netz/start!`, install the middleware, and call `peer/connect`. The executable
socket examples in `test/netz/overlay/integration_test.clj` are the most direct
reference while the API is alpha.

## Public API

| Operation | Meaning |
|---|---|
| `generate-identity` | Create genesis, hot operational key, and recovery material |
| `start!` / `stop!` | Own the Netz runtime lifecycle |
| `install-pubsub!` | Select the overlay as a Kabel pub/sub transport |
| `subscribe!` | Register topic interest and a `PSyncStrategy` |
| `publish!` | Submit a live value; completion is local acceptance, not global delivery |
| `provide!` | Offer an immutable value after content-address verification |
| `fetch!` / `fetch-tree!` | Begin block or DAG retrieval |
| `state` | Inspect protocol state and completion/results |

The fetch API is intentionally still low-level: requests are asynchronous and
results are currently inspected in node state or through the embedding store.
A future portable binding will expose completion streams without leaking the
reference state representation.

## Persistence and secrets

Pass `:store` to persist hot identity material, epoch, address book, and cached
content. Newly generated recovery keys are never written to that store. Either
pass a separately protected `:recovery-store` or capture the one-time
`:recovery-material` returned by `start!`.

Konserve storage encryption is orthogonal to Netz. Encrypted blocks may be
transferred as opaque bytes; key distribution and decryption authorization
belong to an application synchronization profile.
