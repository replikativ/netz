(ns netz.replication-controller-runtime-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :as async :refer [<!! go <!]]
            [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.controller-runtime :as controller-runtime]
            [netz.replication.controller-storage :as controller-storage]
            [netz.replication.intent :as intent]
            [netz.replication.layout :as layout]
            [netz.replication.runtime :as transfer-runtime]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]))

(def private-hex
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def authority (identity/authority-id-bytes genesis))
(def authority-hex (identity/bytes->hex authority))
(def signer
  {:genesis genesis
   :operational {:public (identity/hex->bytes public-hex)
                 :private (identity/hex->bytes private-hex)}})
(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))
(def privacy-domain (identity/hex->bytes (apply str (repeat 16 "20"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))
(def layout0
  {"v" 1 "type" "blob/layout" "cluster" cluster "epoch" 0
   "previous" (identity/hex->bytes (apply str (repeat 32 "00")))
   "members" [{"authority" authority
               "domains" {"owner" "alice"}
               "enabled" true}]
   "issued_at" 900 "signer" authority})
(def retention
  {"v" 1 "type" "blob/retention" "cluster" cluster
   "privacy_domain" privacy-domain "revision" 1
   "manifest" (:manifest-id split) "object" (:object-id split)
   "layout_epoch" 0 "replicas" 1 "write_acks" 1
   "minimum_domains" {"owner" 1} "issued_at" 1000
   "retain_until" 5000 "signer" authority})

(defn await [channel]
  (let [result (<!! channel)]
    (if (instance? Throwable result) (throw result) result)))

(defn sign [statement]
  (await (replication-wire/sign-statement signer statement)))

(defn layout-state [statement]
  (:state (layout/reconcile (layout/make-state cluster [authority]) statement)))

(defrecord MemoryControllerStore [records receipts gate fail?]
  controller-storage/PBlobControllerStore
  (-load-controller-intents [_ _cluster]
    (controller-storage/chan-of
     (->> @records vals (sort-by #(identity/bytes->hex (get % "object"))) vec)))
  (-persist-controller-intent! [_ _cluster object-id record]
    (go
      (when-let [wait @gate] (<! wait))
      (if @fail?
        (ex-info "intent disk failed" {})
        (do (swap! records assoc (identity/bytes->hex object-id) record) :ok))))

  storage/PBlobTransferStore
  (-load-staging [_ _ _ _] (storage/chan-of nil))
  (-stage-piece! [_ _ _ _ _ _ _] (storage/chan-of :ok))
  (-install-object! [_ _ _ _ _ _] (storage/chan-of nil))
  (-load-piece [_ _ _ _ _] (storage/chan-of nil))
  (-persist-receipt! [_ _ _] (storage/chan-of :ok))
  (-recorded-receipts [_] (storage/chan-of @receipts))
  (-cleanup-session! [_ _ _] (storage/chan-of true)))

(defn memory-store
  ([] (memory-store {} []))
  ([records receipts]
   (->MemoryControllerStore (atom records) (atom receipts) (atom nil)
                            (atom false))))

(defn runtime [store state offers callbacks]
  (controller-runtime/start!
   (merge {:store store :cluster cluster :controllers [authority]
           :layout-state-fn #(deref state)
           :dispatch-offer! #(do (swap! offers conj %) true)
           :accepted-persistence #{"netz/store/best-effort/1"}
           :now-fn (constantly 1100)}
          callbacks)))

(defn stored-event [attempt]
  (let [receipt
        (sign
         {"v" 1 "type" "blob/stored" "cluster" cluster
          "privacy_domain" privacy-domain "retention_revision" 1
          "manifest" (:manifest-id split) "object" (:object-id split)
          "layout_epoch" 0 "replica" authority "attempt" attempt
          "persistence" "netz/store/best-effort/1" "stored_at" 1200
          "retain_until" 5000 "signer" authority})]
    {:type :replication/stored
     :object-id (identity/bytes->hex (:object-id split))
     :replica-id authority-hex :layout-epoch 0 :attempt attempt
     :persistence "netz/store/best-effort/1" :receipt receipt
     :manifest-id (identity/bytes->hex (:manifest-id split))}))

(deftest begin-persists-before-dispatch
  (let [store (memory-store)
        gate (async/chan)
        _ (reset! (:gate store) gate)
        state (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store state offers {})
        layout-envelope (sign layout0)
        retention-envelope (sign retention)]
    (is (true? (await (controller-runtime/ready controller))))
    (let [result (controller-runtime/begin!
                  controller layout-envelope retention-envelope
                  (:manifest split))
          [_ port] (async/alts!! [result (async/timeout 50)])]
      (is (not= result port) "begin completed before the outbox write")
      (is (empty? @offers) "an offer escaped before intent persistence")
      (async/put! gate true)
      (is (= :ok (await result)))
      (is (= 1 (count @offers)))
      (is (= 0 (get-in @offers [0 :offer "attempt"])))
      (is (= authority-hex (get-in @offers [0 :replica-id])))
      (is (= 0 (get-in (first (vals @(:records store)))
                       ["intent_revision"])))
      (is (= :exists
             (await (controller-runtime/begin!
                     controller layout-envelope retention-envelope
                     (:manifest split)))))
      (is (= 1 (count @offers))))
    (controller-runtime/stop! controller)))

(deftest failed-intent-write-can-be-retried-without-releasing-an-offer
  (let [store (memory-store)
        _ (reset! (:fail? store) true)
        state (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store state offers {})
        layout-envelope (sign layout0)
        retention-envelope (sign retention)]
    (is (true? (await (controller-runtime/ready controller))))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"intent disk failed"
         (await (controller-runtime/begin!
                 controller layout-envelope retention-envelope
                 (:manifest split)))))
    (is (empty? @offers))
    (is (empty? @(:records store)))
    (reset! (:fail? store) false)
    (is (= :ok
           (await (controller-runtime/begin!
                   controller layout-envelope retention-envelope
                   (:manifest split)))))
    (is (= 1 (count @offers)))
    (controller-runtime/stop! controller)))

(deftest transfer-receipt-replay-can-be-owned-by-the-controller
  (let [event (stored-event 0)
        store (memory-store {} [event])
        node {:identity signer}]
    (with-redefs [netz.overlay.runtime/register-extension!
                  (fn [_ _ _] (constantly true))
                  netz.overlay.runtime/on-stop! (fn [_ _] true)]
      (testing "standalone transfer runtimes preserve replay by default"
        (let [seen (async/chan 1)
              transfer (transfer-runtime/start!
                        node {:store store :on-stored! #(async/put! seen %)})]
          (is (= event (<!! seen)))
          (transfer-runtime/stop! transfer)))
      (testing "composed transfer runtimes leave ordered replay to the controller"
        (let [seen (async/chan 1)
              transfer (transfer-runtime/start!
                        node {:store store :replay-receipts? false
                              :on-stored! #(async/put! seen %)})
              [_ port] (async/alts!! [seen (async/timeout 50)])]
          (is (not= seen port))
          (transfer-runtime/stop! transfer))))))

(deftest restart-replays-receipts-before-explicit-repair
  (let [layout-envelope (sign layout0)
        retention-envelope (sign retention)
        record (intent/make-initial-record
                layout-envelope retention-envelope (:manifest split))
        object-id (identity/bytes->hex (:object-id split))
        receipt (stored-event 0)
        mismatched (assoc receipt :persistence "netz/store/fsync/1")
        store (memory-store {object-id record} [mismatched receipt])
        state (atom (layout-state layout0))
        offers (atom [])
        durable (atom [])
        complete (atom [])
        controller (runtime store state offers
                            {:on-durable! #(swap! durable conj %)
                             :on-complete! #(swap! complete conj %)})]
    (is (true? (await (controller-runtime/ready controller))))
    (is (= #{authority-hex}
           (get-in (controller-runtime/state controller)
                   [:objects object-id :acks])))
    (is (= 1 (count @durable)))
    (is (= 1 (count @complete)))
    (is (empty? @offers))
    (is (= :ok (await (controller-runtime/repair! controller object-id))))
    (is (empty? @offers) "a complete restored intent must not be re-offered")
    (controller-runtime/stop! controller)))

(deftest restart-repair-persists-the-next-attempt-and-fences-layout
  (let [layout-envelope (sign layout0)
        retention-envelope (sign retention)
        record (intent/make-initial-record
                layout-envelope retention-envelope (:manifest split))
        object-id (identity/bytes->hex (:object-id split))
        store (memory-store {object-id record} [])
        state (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store state offers {})]
    (is (true? (await (controller-runtime/ready controller))))
    (let [gate (async/chan)
          _ (reset! (:gate store) gate)
          result (controller-runtime/repair! controller object-id)
          [_ port] (async/alts!! [result (async/timeout 50)])]
      (is (not= result port))
      (is (empty? @offers))
      (async/put! gate true)
      (is (= :ok (await result)))
      (is (= 1 (get-in @offers [0 :offer "attempt"])))
      (is (= 1 (get-in (controller-runtime/records controller)
                       [object-id "intent_revision"]))))

    (testing "a layout advance leaves the old intent durable but inert"
      (let [layout1 (assoc layout0 "epoch" 1
                           "previous" (layout/address layout0)
                           "issued_at" 901)]
        (reset! state
                (:state (layout/reconcile (layout-state layout0) layout1)))
        (is (= :stale-layout
               (await (controller-runtime/repair! controller object-id))))))
    (controller-runtime/stop! controller)))
