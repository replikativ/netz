(ns netz.core
  "Curated public API for the Netz reference implementation.

  Protocol/state-machine namespaces remain public for embedding and testing,
  but application code should start here so their implementation details can
  evolve during the alpha series."
  (:require [kabel.pubsub :as kabel-pubsub]
            [netz.identity :as identity]
            [netz.overlay.runtime :as runtime]
            [netz.pubsub :as overlay-pubsub]))

(def generate-identity identity/generate-identity)
(def start! runtime/start!)
(def stop! runtime/stop!)
(def state runtime/overlay-state)
(def provide! runtime/provide!)
(def fetch! runtime/fetch!)
(def fetch-tree! runtime/fetch-tree!)
(def start-sync! runtime/start-sync!)

(defn install-pubsub!
  "Install the ordered Netz transport on a running Kabel peer."
  [S peer running]
  (overlay-pubsub/install! S peer running))

(defn subscribe!
  "Subscribe with Kabel `PSyncStrategy` instances over the installed overlay."
  [peer topics opts]
  (kabel-pubsub/subscribe! peer topics opts))

(defn publish!
  "Publish through the peer's selected pub/sub transport."
  [peer topic payload]
  (kabel-pubsub/publish! peer topic payload))

(defn unsubscribe!
  [peer topics]
  (kabel-pubsub/unsubscribe! peer topics))
