(ns netz.replication.lifecycle-storage
  "Asynchronous persistence boundary for the blob lifecycle runtime.

  Lifecycle state and transferred bytes can share one Konserve backend, but
  they are separate capabilities. In particular, a backend must not claim a
  successful privacy deletion merely because it hid an installation marker;
  `-delete-object!` has to erase the object's retained storage according to the
  persistence profile it returns."
  (:require [clojure.core.async :refer [chan close! put!]]))

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
