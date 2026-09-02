(ns netz.replication.read-repair
  "Explicit local-first retrieval and read-repair orchestration.

  This layer treats a file or Konserve blob repository as local storage and
  Netz as an explicitly invoked distribution service. It never changes the
  semantics of an ordinary Konserve `get`: callers supply the reconciled signed
  retention statement and an authorized, ordered provider list."
  (:require [netz.identity :as identity]
            [netz.replication.inventory :as inventory]
            [netz.replication.runtime :as runtime]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]
            #?(:clj [clojure.core.async :refer [go <!]]
               :cljs [clojure.core.async :refer [<!] :refer-macros [go]])))

(def max-providers 32)

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- health-result [value]
  (if (failure? value)
    {:status :error
     :code (or (:type (ex-data value)) :storage-error)}
    value))

(defn- provider-key [{:keys [peer authority]}]
  [peer (when (wire/byte-buffer? authority)
          (identity/bytes->hex authority))])

(defn- valid-provider? [{:keys [peer authority]}]
  (and (some? peer)
       (wire/byte-buffer? authority)
       (= 32 (identity/buf-length authority))))

(defn- validate-providers [providers]
  (when-not (and (vector? providers)
                 (<= (count providers) max-providers)
                 (every? valid-provider? providers)
                 (= (count providers)
                    (count (distinct (map provider-key providers)))))
    (throw (ex-info "Invalid or duplicate blob provider list"
                    {:type :netz.replication/invalid-providers
                     :limit max-providers})))
  providers)

(defn- scrub! [store manifest-id]
  (go
    (try
      (<! (inventory/scrub-object! store manifest-id))
      (catch #?(:clj Throwable :cljs :default) cause cause))))

(defn- local-result [manifest-id health]
  {:type :replication/ensure-local
   :status :local
   :manifest-id (identity/bytes->hex manifest-id)
   :health health
   :failures []})

(defn- unavailable-result [manifest-id local failures]
  {:type :replication/ensure-local
   :status :unavailable
   :manifest-id (identity/bytes->hex manifest-id)
   :local (health-result local)
   :failures failures})

(defn ensure-local!
  "Return a channel that proves an object healthy locally or fetches it.

  `retention-envelope` is the already reconciled controller-signed retention
  statement. `providers` is an ordered vector of
  `{:peer transport-peer :authority full-authority-bytes}` selected from the
  authorized layout. At most 32 distinct provider pairs are tried sequentially.

  The default fetch path uses `runtime/fetch-result!`. Tests and higher-level
  schedulers may supply `:fetch-one!`, a function of provider and retention
  envelope returning one terminal result channel. `:attempt` defaults to zero.

  Results have status `:local`, `:fetched`, or `:unavailable`. A corrupt local
  copy does not count as present; verified incoming pieces may repair it, and a
  complete scrub after installation gates `:fetched`."
  ([store replication-runtime providers retention-envelope]
   (ensure-local! store replication-runtime providers retention-envelope {}))
  ([store replication-runtime providers retention-envelope
    {:keys [fetch-one! attempt] :or {attempt 0}}]
   (when-not (satisfies? inventory/PBlobInventoryStore store)
     (throw (ex-info "Read repair requires local inventory and scrubbing"
                     {:type :netz.replication/scrub-not-supported})))
   (when-not (and (integer? attempt) (<= 0 attempt wire/max-safe-integer)
                  (or (nil? fetch-one!) (ifn? fetch-one!)))
     (throw (ex-info "Invalid ensure-local options"
                     {:type :netz.replication/invalid-repair-options})))
   (let [providers (validate-providers providers)
         statement (:statement
                    (replication-wire/parse-envelope retention-envelope))
         _ (replication-wire/validate-statement statement)
         manifest-id (get statement "manifest")
         fetch-one! (or fetch-one!
                        (fn [{:keys [peer authority]} envelope]
                          (runtime/fetch-result!
                           replication-runtime peer authority envelope
                           {:attempt attempt})))]
     (go
       (let [local (<! (scrub! store manifest-id))]
         (if (= :healthy (:status local))
           (local-result manifest-id local)
           (loop [remaining providers failures []]
             (if-let [provider (first remaining)]
               (let [fetched
                     (try
                       (<! (fetch-one! provider retention-envelope))
                       (catch #?(:clj Throwable :cljs :default) cause cause))]
                 (if (= :installed (:status fetched))
                   (let [health (<! (scrub! store manifest-id))]
                     (if (= :healthy (:status health))
                       {:type :replication/ensure-local
                        :status :fetched
                        :manifest-id (identity/bytes->hex manifest-id)
                        :provider-id (identity/bytes->hex
                                      (:authority provider))
                        :health health
                        :fetch fetched
                        :local-before (health-result local)
                        :failures failures}
                       (recur (next remaining)
                              (conj failures
                                    {:provider-id
                                     (identity/bytes->hex
                                      (:authority provider))
                                     :code :post-fetch-scrub-failed
                                     :detail (health-result health)}))))
                   (recur (next remaining)
                          (conj failures
                                {:provider-id
                                 (identity/bytes->hex (:authority provider))
                                 :code (if (failure? fetched)
                                         :fetch-error
                                         (or (:code fetched)
                                             :invalid-fetch-result))}))))
               (unavailable-result manifest-id local failures)))))))))
