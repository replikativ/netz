(ns netz.replication.layout-storage
  "Asynchronous persistence boundary for signed replica-layout transcripts."
  (:require [clojure.core.async :refer [chan close! put!]]))

(defprotocol PBlobLayoutStore
  (-load-layout-transcript [store cluster]
    "Yield nil or the ordered vector of signed layout envelopes for `cluster`.")

  (-persist-layout-transcript! [store cluster envelopes]
    "Atomically persist the complete ordered envelope vector and yield `:ok`
    only at the store's configured durability boundary."))

(defn chan-of [value]
  (let [result (chan 1)]
    (when (some? value) (put! result value))
    (close! result)
    result))
