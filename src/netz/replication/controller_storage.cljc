(ns netz.replication.controller-storage
  "Asynchronous persistence boundary for durable replication intents."
  (:require [clojure.core.async :refer [chan close! put!]]))

(defprotocol PBlobControllerStore
  (-load-controller-intents [store cluster]
    "Yield the bounded vector of durable intent records for `cluster`.")

  (-persist-controller-intent! [store cluster object-id record]
    "Atomically persist the complete current intent record for `object-id` and
    yield `:ok` only at the store's configured durability boundary."))

(defn chan-of [value]
  (let [result (chan 1)]
    (when (some? value) (put! result value))
    (close! result)
    result))
