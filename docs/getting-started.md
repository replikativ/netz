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

## Authenticated transport

The public-network path uses Kabel CBOR inside Noise through the released Kabel
and Geheimnis coordinates in `deps.edn`. Construct the peer with the same
identity that starts the overlay:

```clojure
(require '[kabel.middleware.cbor :refer [cbor]]
         '[netz.transport.secure :as secure])

(let [[application install!] (runtime/deferred-middleware)
      socket-peer
      (peer/client-peer
       S peer-id application cbor (atom {}) (atom {})
       {:transport-middleware
        (secure/middleware {:identity keys
                            :addresses []
                            :seq-no 1})})
      node
      (<?? S
           (secure/start!
            S socket-peer
            {:identity keys
             :topics #{:example}
             :seeds [{:peer-id seed-routing-id
                      :authority-id seed-full-authority-id
                      :addresses [seed-url]}]
             :overlay-opts
             {:membership {:require-authority? true}}}))]
  (install! (:middleware node)))
```

`secure/start!` registers the identity already verified inside the Noise
transcript and does not emit the transitional public hello/proof exchange. A
secure seed therefore needs both the UUID-shaped routing ID and the full
64-hex-character Authority ID. The runtime passes the latter as a per-dial
Kabel context value, so connecting to the right URL with the wrong key fails
closed.

### Portable dissemination

Portable dissemination is explicit and requires all three capability tokens on
both Noise peers:

```clojure
(require '[netz.dissemination-wire :as dissemination-wire]
         '[netz.overlay.runtime :as runtime]
         '[netz.transport.noise :as noise]
         '[netz.wire :as wire])

(def portable-dissemination
  (conj noise/default-capabilities
        wire/profile-capability
        runtime/extension-carrier-capability
        dissemination-wire/capability))
```

Pass that set as `:capabilities` to `secure/middleware`, and provide an explicit
policy to `secure/start!`:

```clojure
{:topics #{"datahike/roots/example"}
 :authorize-dissemination?
 (fn [{:keys [direction authority-id origin-authority topic frame-type]}]
   ;; Consult application policy here. The result may be a boolean or a
   ;; core.async channel. This example is deliberately narrow.
   (and (= authority-id configured-remote-authority)
        (or (nil? topic)
            (= topic "datahike/roots/example"))))
 :authorization-timeout-ms 5000}
```

The hook runs for inbound and outbound interests, repair control, and
publications. `authority-id` is the immediate authenticated connection peer;
`origin-authority` is additionally present for a publication and may differ
after relaying. Missing, false, exceptional, closed-channel, or timed-out
decisions fail closed. The validated `:frame` is available when policy needs to
inspect every topic in an interests or repair-control frame. Profile-1 topics
are non-empty UTF-8 strings and
publication payloads are opaque byte strings; Clojure keyword topics and
arbitrary values remain part of the transitional native profile.

### Portable content service

Portable block serving is a separately negotiated capability. Add
`netz.content-wire/capability` to the same wire and extension-carrier set, then
install the service after starting the overlay:

```clojure
(require '[netz.content-runtime :as content-runtime]
         '[netz.content-wire :as content-wire])

(def stop-content!
  (content-runtime/start-service!
   node
   {:authorize?
    (fn [{:keys [op authority-id cid]}]
      (and (= op :content/read)
           (= authority-id configured-remote-authority)
           (allowed-cid? cid)))
    :get-block
    (fn [cid]
      ;; Return opaque bytes, nil, or a core.async channel yielding either.
      (load-cid cid))
    :find-providers
    (fn [cid]
      ;; Optional full 32-byte Authority IDs; [] discloses no hints.
      (known-providers cid))}))
```

The request queue, authorization wait, backend wait, response sizes, and
provider arrays are bounded. Policy runs before either callback, and returned
block bytes are hashed again before they cross the connection. Calling the
returned function unregisters the capability handler and stops its worker.
Stopping the overlay does this automatically.

## Public API

| Operation | Meaning |
|---|---|
| `generate-identity` | Create genesis, hot operational key, and recovery material |
| `start!` / `stop!` | Own the Netz runtime lifecycle |
| `start-content-service!` | Install authorization-first portable CID serving |
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

## Public gateways and address policy

The default address predicate accepts absolute credential-free `ws` and `wss`
URIs because private networks and local peers are normal deployments. It is not
an SSRF boundary.

A public gateway should pass an exact allowlist:

```clojure
{:allow-address?
 (runtime/public-gateway-address-policy
  [{:peer-id configured-routing-id
    :addresses ["wss://peer.example/netz"]}])}
```

This permits only the operator-selected peer/address pair over TLS; peer
exchange cannot add a dial target. The operator controls the DNS of that
allowlist. Dynamically dialing discovered hostnames at a public boundary is
unsupported until the WebSocket adapter can validate and pin the resolved IP
used by the socket. Resolving first and checking separately is insufficient
because DNS can change between the check and connection. WebSocket redirects
must not be enabled to targets outside the same allowlist.
