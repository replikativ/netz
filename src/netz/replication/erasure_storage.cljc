(ns netz.replication.erasure-storage
  "Asynchronous persistence boundary for controller-side erasure campaigns."
  (:require [clojure.core.async :refer [chan close! put!]]))

(defprotocol PBlobErasureStore
  (-load-erasure-records [store cluster]
    "Yield the bounded map of erasure key to durable campaign record.")

  (-persist-erasure-record! [store cluster key record]
    "Atomically persist one complete campaign record and yield `:ok` only at
    the configured durability boundary."))

(defn record->wire
  "Encode the pure coordinator's record as a language-neutral CBOR value."
  [record]
  {"v" 1 "type" "blob/erasure-campaign"
   "key" (:key record)
   "statement" (:statement record)
   "fingerprint" (:fingerprint record)
   "deletion" (:deletion-envelope record)
   "holders" (:holders record)
   "receipts" (:receipts record)
   "attempted_at" (:attempted-at record)})

(defn wire->record
  "Decode one exact language-neutral campaign record for pure-state restore."
  [value]
  (when-not (and (map? value)
                 (= #{"v" "type" "key" "statement" "fingerprint"
                      "deletion" "holders" "receipts" "attempted_at"}
                    (set (keys value)))
                 (= 1 (get value "v"))
                 (= "blob/erasure-campaign" (get value "type")))
    (throw (ex-info "Invalid portable erasure record"
                    {:type :netz.replication/invalid-erasure-record})))
  {:key (get value "key")
   :statement (get value "statement")
   :fingerprint (get value "fingerprint")
   :deletion-envelope (get value "deletion")
   :holders (get value "holders")
   :receipts (get value "receipts")
   :attempted-at (get value "attempted_at")})

(defn chan-of [value]
  (let [result (chan 1)]
    (when (some? value) (put! result value))
    (close! result)
    result))
