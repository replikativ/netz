(ns netz.store.konserve
  "konserve-backed `PPeerStore`.

  Durable overlay state — the keypair, the address book, provider records —
  wherever kabel runs, since konserve reaches the JVM, Node and the browser
  (IndexedDB). A browser peer therefore keeps its identity across reloads,
  which is the difference between a peer and a visitor.

  konserve is **not** a dependency of kabel's base library; this namespace
  lives behind the `:konserve` alias, mirroring how `kabel.auth.store.datahike`
  treats datahike. See `netz.store.protocol` for the argument.

  The peer store is deliberately a *small* namespace inside whatever konserve
  store you hand it: keys are the fixed vocabulary from the protocol, not
  arbitrary application data. Pass a `:prefix` if the store is shared with
  something else."
  (:require [netz.store.protocol :refer [PPeerStore]]
            [konserve.core :as k]
            [superv.async :refer [<?- go-try-] :include-macros true]))

(defrecord KonservePeerStore [store prefix]
  PPeerStore

  (-load [_ k]
    (k/get-in store [prefix k] nil {:sync? false}))

  (-store! [_ k v]
    (go-try-
     (<?- (k/assoc-in store [prefix k] v {:sync? false}))
     v))

  (-remove! [_ k]
    (go-try-
     (<?- (k/update store prefix #(dissoc % k) {:sync? false}))
     true))

  (-keys* [_]
    (go-try-
     (set (keys (<?- (k/get store prefix nil {:sync? false})))))))

(defn new-konserve-store
  "Wrap a konserve store as a `PPeerStore`.

  `:prefix` is the top-level konserve key the overlay's state lives under, so
  a peer can share a store with an application without colliding with it.
  Defaults to `:netz.overlay`."
  ([store] (new-konserve-store store {}))
  ([store {:keys [prefix]}]
   (->KonservePeerStore store (or prefix :netz.overlay))))
