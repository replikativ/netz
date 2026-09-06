(ns netz.examples.snapshot-transfer
  "Experimental system-specific closure boundary above Netz and Konserve.
   Blocking JVM example API; not yet a new Yggdrasil protocol or wire standard."
  (:require [clojure.core.async :as a]
            [clojure.set :as set]
            [kabel.pubsub.protocol :as protocol]
            [konserve.core :as k]
            [konserve-sync.pubsub :as ks]
            [netz.examples.snapshot-store :as snapshot]
            [netz.identity :as id]
            [netz.snapshot-ref :as ref]))

(defprotocol SnapshotTransfer
  (select-snapshot! [adapter event]
    "Return {:source Konserve-view :keys [...] :root-key key :profile string}.
     Select the exact event snapshot, never a newer mutable head. The caller
     must prevent GC until capture! returns. Keys must be immutable and form
     the complete closure needed by open-snapshot!.")
  (validate-root! [adapter root-key statement]
    "Reject incompatible profiles and root bindings before installing values.")
  (open-snapshot! [adapter target root-key]
    "Validate the installed closure and return the native read view. Do not
     advance application branches. Caller owns target GC/retention."))

(defn capture! [adapter event authority encode parents]
  (let [{:keys [source keys root-key profile]} (select-snapshot! adapter event)
        signed (snapshot/await! (ref/sign-dag-event authority profile event))]
    (validate-root! adapter root-key signed)
    (snapshot/export! source keys root-key signed authority encode parents)))

(defn install!
  "Install a finite authenticated catalogue through konserve-sync.
   Every value must verify on read-back; immutable conflicts fail closed.
   No success on a truncated legacy stream, even if it closes without an error."
  [adapter {:keys [source root-key root-statement]} target encode]
  (validate-root! adapter root-key root-statement)
  (let [sender (ks/server-store-strategy source {:key-sort-fn #(if (= % root-key) 1 0)})
        receiver (ks/store-sync-strategy target {})
        produced (protocol/-handshake-items sender {})
        stream (if (map? produced) produced {:items produced})
        expected (set (map :key (snapshot/await! (k/keys source))))
        seen (atom #{})]
    (try
      (loop []
        (when-let [item (snapshot/await! (:items stream))]
          (when (or (not (contains? expected (:key item))) (contains? @seen (:key item)))
            (throw (ex-info "Unexpected or duplicate snapshot key" {:key (:key item)})))
          (when (and (= root-key (:key item)) (not= @seen (disj expected root-key)))
            (throw (ex-info "Snapshot root arrived before its complete closure" {})))
          (when (instance? Throwable (:value item)) (throw (:value item)))
          (if (snapshot/await! (k/exists? target (:key item)))
            (when-not (id/bufs= (encode (:value item))
                                (encode (snapshot/await! (k/get target (:key item)))))
              (throw (ex-info "Conflicting immutable snapshot key" {:key (:key item)})))
            (let [result (snapshot/await! (protocol/-apply-handshake-item receiver item))]
              (when-not (= true (:ok result))
                (throw (ex-info "Konserve snapshot install failed" {:result result})))
              (when-not (id/bufs= (encode (:value item))
                                  (encode (snapshot/await! (k/get target (:key item)))))
                (throw (ex-info "Snapshot write failed read-back verification" {:key (:key item)})))))
          (swap! seen conj (:key item))
          (recur)))
      (when-let [completion (:completion stream)]
        (let [result (snapshot/await! completion)]
          (when-not (= true (:ok result))
            (throw (ex-info "Konserve snapshot source failed" {:result result})))))
      (when-not (= expected @seen)
        (throw (ex-info "Incomplete snapshot source" {:missing (set/difference expected @seen)})))
      (finally (a/close! (:items stream)))))
  (open-snapshot! adapter target root-key))
