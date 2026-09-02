(ns netz.store.konserve-replication
  "Explicit optional Konserve seam for quorum-replicated immutable values.

  This is intentionally a function, not yet a transparent PStore wrapper. A
  wrapper has to define nested updates, binary stream replay, multi-key
  ordering, read repair, and every optional Konserve protocol. Hiding those
  decisions in alpha code would make an ordinary `assoc` acquire surprising
  network semantics.

  `assoc-immutable!` makes the important ordering exact: local Konserve commit
  first, then a caller-supplied Netz replicator which owns its durable outbox
  and quorum receipt. The returned channel does not complete before both have
  produced results."
  (:require [konserve.core :as k]
            #?(:clj [clojure.core.async :refer [go <!]]
               :cljs [clojure.core.async :refer [<!] :refer-macros [go]])))

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn assoc-immutable!
  "Store one immutable EDN value locally, then request Netz durability.

  `replicate!` receives `{:key key :value value :meta meta}` after local commit
  and returns a channel yielding a durability receipt. The result channel
  yields `{:local-result [old new] :receipt receipt}`. A quorum timeout should
  be represented honestly in `receipt`, including `:local-committed? true`.

  A local Konserve failure is delivered unchanged and `replicate!` is not
  invoked. A thrown or channel-delivered replication failure is likewise
  delivered as the terminal channel value.

  This function always uses Konserve's async API. `meta` is extended with
  `:immutable? true`; callers cannot accidentally label the durable-network
  operation mutable."
  ([store replicate! key value]
   (assoc-immutable! store replicate! key value {}))
  ([store replicate! key value meta]
   (let [meta (assoc (or meta {}) :immutable? true)]
     (go
       (try
         (let [local-result (<! (k/assoc store key value meta {:sync? false}))]
           (if (failure? local-result)
             local-result
             (let [receipt
                   (<! (replicate! {:key key :value value :meta meta}))]
               (if (failure? receipt)
                 receipt
                 {:local-result local-result
                  :receipt receipt}))))
         (catch #?(:clj Throwable :cljs :default) cause cause))))))
