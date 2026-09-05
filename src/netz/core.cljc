(ns netz.core
  "Curated public API for the Netz reference implementation.

  Protocol/state-machine namespaces remain public for embedding and testing,
  but application code should start here so their implementation details can
  evolve during the alpha series."
  (:require [kabel.pubsub :as kabel-pubsub]
            [netz.content-runtime :as content-runtime]
            [netz.identity :as identity]
            [netz.overlay.runtime :as runtime]
            [netz.pubsub :as overlay-pubsub]
            [netz.snapshot-ref :as snapshot-ref]
            [netz.snapshot-ref.pubsub :as snapshot-ref-pubsub]))

(def generate-identity identity/generate-identity)
(def start! runtime/start!)
(def stop! runtime/stop!)
(def state runtime/overlay-state)
(def provide! runtime/provide!)
(def fetch! runtime/fetch!)
(def fetch-tree! runtime/fetch-tree!)
(def start-sync! runtime/start-sync!)
(def start-content-service! content-runtime/start-service!)
(def start-content-requester! content-runtime/start-requester!)
(def sign-snapshot-ref snapshot-ref/sign-dag-event)
(def snapshot-ref-topic snapshot-ref/topic-for)
(def snapshot-ref-strategy snapshot-ref-pubsub/strategy)

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
  "Cancel Netz intent and orderly-drain the selected Kabel generation."
  [peer topics]
  (overlay-pubsub/unsubscribe! peer topics))
