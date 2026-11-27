(ns netz.replication.lifecycle-storage
  "Asynchronous persistence boundary for the blob lifecycle runtime.

  Lifecycle state and transferred bytes can share one Konserve backend, but
  they are separate capabilities. In particular, a backend must not claim a
  successful privacy deletion merely because it hid an installation marker;
  `-delete-object!` has to erase the object's retained storage according to the
  persistence profile it returns."
  (:require [clojure.core.async :refer [chan close! put!]]))

(def ^:private record-fields
  #{:statement :controller :revision :fingerprint :status :expired-at
    :retry-at :deleted-at :delete-persistence :receipt-body
    :receipt-envelope})

(def ^:private wire-record-fields
  (set (map name record-fields)))

(def ^:private required-record-fields
  #{:statement :controller :revision :fingerprint :status})

(def ^:private lifecycle-statuses
  #{:retained :deletion-pending :deleting :expiring :deletion-failed
    :deleted})

(defn- object-key? [key]
  (and (string? key)
       (boolean
        (re-matches #"(?:[0-9a-f]{2}){1,64}/(?:[0-9a-f]{2}){1,64}/[0-9a-f]{64}" key))))

(defn record->wire
  "Encode one lifecycle record as a closed, language-neutral CBOR value."
  [key record]
  (when-not (and (object-key? key) (map? record)
                 (every? record-fields (keys record))
                 (every? #(contains? record %) required-record-fields)
                 (map? (:statement record))
                 (string? (:controller record))
                 (integer? (:revision record))
                 (string? (:fingerprint record))
                 (contains? lifecycle-statuses (:status record)))
    (throw (ex-info "Invalid lifecycle storage record"
                    {:type ::invalid-record})))
  {"v" 1 "type" "blob/lifecycle-record" "key" key
   "record" (into {}
                  (map (fn [[field value]]
                         [(name field)
                          (if (= field :status) (name value) value)]))
                  record)})

(defn wire->record
  "Decode the portable lifecycle wrapper into `[object-key record]`."
  [value]
  (when-not (and (map? value)
                 (= #{"v" "type" "key" "record"} (set (keys value)))
                 (= 1 (get value "v"))
                 (= "blob/lifecycle-record" (get value "type"))
                 (object-key? (get value "key"))
                 (map? (get value "record")))
    (throw (ex-info "Invalid lifecycle storage wrapper"
                    {:type ::invalid-record})))
  (let [wire-record (get value "record")]
    (when-not (and (every? string? (keys wire-record))
                   (every? wire-record-fields (keys wire-record))
                   (every? #(contains? wire-record (name %))
                           required-record-fields)
                   (map? (get wire-record "statement"))
                   (string? (get wire-record "controller"))
                   (integer? (get wire-record "revision"))
                   (string? (get wire-record "fingerprint"))
                   (contains? (set (map name lifecycle-statuses))
                              (get wire-record "status")))
      (throw (ex-info "Invalid lifecycle storage fields"
                      {:type ::invalid-record})))
    (let [record (into {}
                       (map (fn [[field value]]
                              (let [field (keyword field)]
                                [field (if (= field :status)
                                         (keyword value) value)])))
                       wire-record)]
      [(get value "key") record])))

(defprotocol PBlobLifecycleStore
  (-load-lifecycle-records [store]
    "Yield the bounded map of object-key to durable lifecycle record.")

  (-persist-lifecycle! [store key value]
    "Persist one lifecycle journal value and yield `:ok` only at the store's
    configured durability boundary. Keys are the pure machine's namespaced
    vectors, such as `[:blob/lifecycle object-key]`.")

  (-delete-object! [store key statement requested-persistence]
    "Erase all locally retained bytes for `statement` and yield
    `{:result :ok :persistence profile :deleted-at unix-ms}`. A marker-only
    removal or deferred garbage collection must yield a failure instead."))

(defn chan-of
  "Return a closed channel containing `value`; nil closes it empty."
  [value]
  (let [result (chan 1)]
    (when (some? value) (put! result value))
    (close! result)
    result))
