(ns netz.examples.static-bootstrap
  "Small-deployment EDN bootstrap for the Datahike process experiment.

  EDN is an operator format, not a Netz wire profile. The embedded layout is
  still the canonical-CBOR signed authority. Callers pin the expected network
  and controller authorities outside the fetched file, so replacing the file
  can redirect or withhold seeds but cannot replace the trust root."
  (:require [clojure.core.async :refer [go <!]]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [netz.identity :as identity]
            [netz.overlay.runtime :as runtime]
            [netz.replication.layout :as layout]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]))

(def roles #{:writer :replica :cache :gateway})
(def fields #{:v :network :issued-at :expires-at :controllers :peers
              :layout-chain-cbor-hex})
(def peer-fields #{:authority-id :peer-id :addresses :group :domains :roles})

(defn- authority-id? [value]
  (and (string? value) (boolean (re-matches #"[0-9a-f]{64}" value))))

(defn- valid-peer? [peer]
  (and (= peer-fields (set (keys peer)))
       (authority-id? (:authority-id peer))
       (= (str (identity/authority-routing-id (:authority-id peer))) (:peer-id peer))
       (vector? (:addresses peer)) (seq (:addresses peer))
       (every? runtime/websocket-address? (:addresses peer))
       (string? (:group peer)) (not-empty (:group peer))
       (map? (:domains peer))
       (every? string? (keys (:domains peer)))
       (every? #(and (string? %) (not-empty %)) (vals (:domains peer)))
       (vector? (:roles peer)) (seq (:roles peer))
       (set/subset? (set (:roles peer)) roles)))

(defn read-file
  "Read a closed, data-only EDN bootstrap. No custom tagged literals execute."
  [path]
  (let [value (edn/read-string
               {:readers {}
                :default (fn [tag _]
                           (throw (ex-info "Tagged bootstrap value is forbidden"
                                           {:tag tag})))}
               (slurp (str path)))]
    (when-not
     (and (map? value) (= fields (set (keys value))) (= 1 (:v value))
          (string? (:network value)) (not-empty (:network value))
          (integer? (:issued-at value)) (integer? (:expires-at value))
          (< (:issued-at value) (:expires-at value))
          (vector? (:controllers value)) (seq (:controllers value))
          (every? authority-id? (:controllers value))
          (= (count (:controllers value)) (count (set (:controllers value))))
          (vector? (:peers value)) (seq (:peers value))
          (every? valid-peer? (:peers value))
          (= (count (:peers value))
             (count (set (map :authority-id (:peers value))))
             (count (set (map :peer-id (:peers value)))))
          (vector? (:layout-chain-cbor-hex value))
          (<= 1 (count (:layout-chain-cbor-hex value)) 32)
          (every? #(and (string? %) (even? (count %)))
                  (:layout-chain-cbor-hex value)))
      (throw (ex-info "Invalid static Netz bootstrap"
                      {:type :netz.bootstrap/invalid})))
    value))

(defn load-and-verify!
  "Yield validated bootstrap plus its accepted signed layout state.

  `trust` is local configuration and MUST NOT come from the fetched bootstrap."
  [path now {:keys [network controllers]}]
  (let [bootstrap (read-file path)]
    (go
      (try
        (when-not (and (string? network)
                       (seq network)
                       (= network (:network bootstrap))
                       (set? controllers)
                       (seq controllers)
                       (every? authority-id? controllers)
                       (= controllers (set (:controllers bootstrap))))
          (throw (ex-info "Static Netz bootstrap disagrees with local trust"
                          {:type :netz.bootstrap/trust-mismatch})))
        (when-not (<= (:issued-at bootstrap) now (:expires-at bootstrap))
          (throw (ex-info "Static Netz bootstrap is not current"
                          {:type :netz.bootstrap/expired})))
        (let [envelopes
              (mapv #(wire/decode (identity/hex->bytes %))
                    (:layout-chain-cbor-hex bootstrap))
              peers (into {} (map (juxt :authority-id identity)) (:peers bootstrap))
              statements
              (loop [remaining envelopes result []]
                (if-let [envelope (first remaining)]
                  (let [verified? (<! (replication-wire/verify-envelope envelope))
                        statement (:statement
                                   (replication-wire/parse-envelope envelope))
                        signer (identity/bytes->hex (get statement "signer"))]
                    (when-not (and verified? (contains? controllers signer))
                      (throw (ex-info "Bootstrap layout is not controller-authorized"
                                      {:type :netz.bootstrap/unauthorized-layout})))
                    (recur (next remaining) (conj result statement)))
                  result))]
          (doseq [statement statements
                  member (get statement "members")]
            (let [authority (identity/bytes->hex (get member "authority"))
                  peer (get peers authority)]
              (when-not (and peer (= (get member "domains") (:domains peer)))
                (throw (ex-info "Bootstrap peer disagrees with signed layout"
                                {:type :netz.bootstrap/layout-peer-mismatch
                                 :authority authority})))))
          (let [cluster (get (first statements) "cluster")
                initial (layout/make-state
                         cluster (mapv identity/hex->bytes controllers))
                state
                (reduce
                 (fn [state statement]
                   (let [result (layout/reconcile state statement)]
                     (when-not (= :accepted (:status result))
                       (throw (ex-info "Bootstrap layout chain was not accepted"
                                       {:type :netz.bootstrap/rejected-layout
                                        :epoch (get statement "epoch")
                                        :status (:status result)})))
                     (:state result)))
                 initial statements)]
            (when (seq (:conflicts state))
              (throw (ex-info "Bootstrap layout chain is forked"
                              {:type :netz.bootstrap/forked-layout})))
            (assoc bootstrap :layout-envelopes envelopes :layout-state state)))
        (catch Throwable cause cause)))))

(defn seeds
  "Convert bootstrap peers to the authenticated overlay seed shape."
  [bootstrap]
  (mapv (fn [{:keys [authority-id peer-id addresses group]}]
          {:authority-id authority-id
           :peer-id (java.util.UUID/fromString peer-id)
           :addresses addresses :group group})
        (:peers bootstrap)))
