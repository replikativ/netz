(ns netz.replication-controller-runtime-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :as async :refer [<!! go <!]]
            [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.audit :as audit]
            [netz.replication.audit-runtime :as audit-runtime]
            [netz.replication.controller-runtime :as controller-runtime]
            [netz.replication.connectivity-runtime :as connectivity-runtime]
            [netz.replication.controller-storage :as controller-storage]
            [netz.replication.intent :as intent]
            [netz.replication.layout :as layout]
            [netz.replication.repair-scheduler-runtime :as repair-scheduler-runtime]
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
   "placement" "netz/blob-placement/2"
   "seed" (identity/hex->bytes (apply str (repeat 32 "aa")))
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
   "placement_minimum_domains" {"owner" 1}
   "ack_minimum_domains" {"owner" 1} "issued_at" 1000
   "retain_until" 5000 "signer" authority})

(defn await [channel]
  (let [result (<!! channel)]
    (if (instance? Throwable result) (throw result) result)))

(defn wait-for [ms predicate]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (if (predicate)
        true
        (when (< (System/currentTimeMillis) deadline)
          (Thread/sleep 2)
          (recur))))))

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

(defn stored-event
  ([attempt] (stored-event retention attempt))
  ([retention-statement attempt]
   (let [receipt
         (sign
          {"v" 1 "type" "blob/stored" "cluster" cluster
           "privacy_domain" privacy-domain
           "retention_revision" (get retention-statement "revision")
           "manifest" (:manifest-id split) "object" (:object-id split)
           "layout_epoch" (get retention-statement "layout_epoch")
           "replica" authority "attempt" attempt
           "persistence" "netz/store/best-effort/1" "stored_at" 1200
           "retain_until" (get retention-statement "retain_until")
           "signer" authority})]
     {:type :replication/stored
      :object-id (identity/bytes->hex (:object-id split))
      :replica-id authority-hex
      :layout-epoch (get retention-statement "layout_epoch")
      :attempt attempt
      :persistence "netz/store/best-effort/1" :receipt receipt
      :manifest-id (identity/bytes->hex (:manifest-id split))})))

(deftest connectivity-coordinator-refreshes-and-releases-current-targets
  (let [store (memory-store)
        state (atom (layout-state layout0))
        controller (runtime store state (atom []) {})
        overlay {:events (async/chan 4) :stopped? (atom false)
                 :state (atom {:membership {:opts {:max-demand 32}}})}]
    (try
      (await (controller-runtime/ready controller))
      (await (controller-runtime/begin! controller (sign layout0) (sign retention) (:manifest split)))
      (let [coordinator (connectivity-runtime/start!
                         {:overlay overlay :controllers [controller] :now-fn (constantly 1000)})]
        (try
          (let [event (await (:events overlay))]
            (is (= :connection-demand (:type event)))
            (is (= {(identity/authority-routing-id authority-hex) authority-hex} (:demand event)))
            (is (= 6000 (:until event))))
          (is (true? (await (connectivity-runtime/stop! coordinator))))
          (is (= {} (:demand (await (:events overlay)))))
          (finally (await (connectivity-runtime/stop! coordinator)))))
      (testing "a full overlay queue neither blocks refresh nor shutdown"
        (dotimes [_ 4] (is (async/offer! (:events overlay) :occupied)))
        (let [coordinator (connectivity-runtime/start!
                           {:overlay overlay :controllers [controller] :now-fn (constantly 1000)})]
          (try
            (is (wait-for 1000 #(pos? (:refused (connectivity-runtime/stats coordinator)))))
            (is (true? (await (connectivity-runtime/stop! coordinator))))
            (is (= 0 (:submitted (connectivity-runtime/stats coordinator))))
            (is (= (repeat 4 :occupied) (repeatedly 4 #(async/poll! (:events overlay)))))
            (is (nil? (async/poll! (:events overlay))))
            (finally (await (connectivity-runtime/stop! coordinator))))))
      (finally (controller-runtime/stop! controller) (async/close! (:events overlay))))))

(deftest connection-targets-drop-ineligible-intents
  (let [state (atom (layout-state layout0))
        now (atom 1100)
        controller (runtime (memory-store) state (atom []) {:now-fn #(deref now)})]
    (try
      (await (controller-runtime/ready controller))
      (await (controller-runtime/begin! controller (sign layout0) (sign retention) (:manifest split)))
      (is (= #{authority-hex} (controller-runtime/connection-targets controller)))
      (reset! now (get retention "retain_until"))
      (is (empty? (controller-runtime/connection-targets controller)))
      (reset! now 1100)
      (swap! state assoc :conflicts [{:epoch 0}])
      (is (empty? (controller-runtime/connection-targets controller)))
      (reset! state (layout-state layout0))
      (let [successor (assoc layout0 "epoch" 1 "previous" (layout/address layout0)
                             "issued_at" 1100)]
        (reset! state (:state (layout/reconcile @state successor)))
        (is (= 1 (get-in @state [:current :statement "epoch"])))
        (is (empty? (controller-runtime/connection-targets controller))))
      (reset! state (layout-state layout0))
      (controller-runtime/stop! controller)
      (is (empty? (controller-runtime/connection-targets controller)))
      (finally (controller-runtime/stop! controller)))))

(deftest active-revalidation-is-durable-fenced-and-awaitable
  (let [store (memory-store)
        state (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store state offers {})
        object-id (identity/bytes->hex (:object-id split))]
    (try
      (await (controller-runtime/ready controller))
      (is (empty? (controller-runtime/connection-targets controller)))
      (await (controller-runtime/begin! controller (sign layout0) (sign retention) (:manifest split)))
      (is (= #{authority-hex} (controller-runtime/connection-targets controller)))
      (controller-runtime/notify-stored! controller (stored-event 0))
      (is (= :confirmed (:status (await (controller-runtime/await-durable! controller object-id)))))
      (is (empty? (controller-runtime/connection-targets controller)))
      (let [observation (first (audit/observations (controller-runtime/state controller)))
            gate (async/chan)]
        (reset! (:gate store) gate)
        (let [result (controller-runtime/revalidate! controller observation)]
          (is (wait-for 1000 #(= :pending (:status (controller-runtime/durability-status controller object-id)))))
          (is (= 1 (count @offers)) "no repair dispatch before persistence")
          (async/put! gate true)
          (is (= :ok (await result))))
        (is (= 1 (get-in (last @offers) [:offer "attempt"])))
        (is (= :stale-observation (await (controller-runtime/revalidate! controller observation))))
        (controller-runtime/notify-stored! controller (stored-event 0))
        (let [result (await (controller-runtime/await-durable! controller object-id {:timeout-ms 30 :poll-ms 5}))]
          (is (= :timeout (:status result)))
          (is (= :under-replicated (:last-status result))))
        (controller-runtime/notify-stored! controller (stored-event 1))
        (is (= :confirmed (:status (await (controller-runtime/await-durable! controller object-id))))))
      (finally (controller-runtime/stop! controller)))))

(deftest audit-runtime-actively-reoffers-previously-complete-object
  (let [store (memory-store)
        state (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store state offers {})
        object-id (identity/bytes->hex (:object-id split))]
    (try
      (await (controller-runtime/ready controller))
      (await (controller-runtime/begin! controller (sign layout0) (sign retention) (:manifest split)))
      (controller-runtime/notify-stored! controller (stored-event 0))
      (await (controller-runtime/await-durable! controller object-id))
      (let [auditor (audit-runtime/start! {:controller controller :scan-ms 5
                                           :audit-opts {:interval-ms 10}})]
        (try
          (is (true? (await (audit-runtime/ready auditor))))
          (is (wait-for 2000 #(= 2 (count @offers))))
          (is (= :under-replicated (:status (controller-runtime/durability-status controller object-id))))
          (finally (await (audit-runtime/stop! auditor)))))
      (finally (controller-runtime/stop! controller)))))

(deftest revalidation-restart-does-not-resurrect-old-receipts
  (let [store (memory-store)
        state (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store state offers {})
        object-id (identity/bytes->hex (:object-id split))]
    (try
      (await (controller-runtime/ready controller))
      (await (controller-runtime/begin! controller (sign layout0) (sign retention) (:manifest split)))
      (reset! (:receipts store) [(stored-event 0)])
      (controller-runtime/notify-stored! controller (stored-event 0))
      (await (controller-runtime/await-durable! controller object-id))
      (is (= :ok (await (controller-runtime/revalidate! controller
                                                        (first (audit/observations (controller-runtime/state controller)))))))
      (finally (controller-runtime/stop! controller)))
    (let [restarted (runtime store state (atom []) {})]
      (try
        (await (controller-runtime/ready restarted))
        (is (= :under-replicated (:status (controller-runtime/durability-status restarted object-id))))
        (is (= 1 (get-in (controller-runtime/state restarted) [:objects object-id :attempts authority-hex])))
        (finally (controller-runtime/stop! restarted))))))

(deftest expired-and-forked-layout-evidence-does-not-confirm
  (let [store (memory-store)
        state (atom (layout-state layout0))
        now (atom 1100)
        controller (runtime store state (atom []) {:now-fn #(deref now)})
        object-id (identity/bytes->hex (:object-id split))]
    (try
      (await (controller-runtime/ready controller))
      (await (controller-runtime/begin! controller (sign layout0) (sign retention) (:manifest split)))
      (controller-runtime/notify-stored! controller (stored-event 0))
      (is (= :confirmed (:status (await (controller-runtime/await-durable! controller object-id)))))
      (reset! now 5000)
      (is (= :inactive (:status (controller-runtime/durability-status controller object-id))))
      (reset! now 1100)
      (swap! state assoc :conflicts [{:epoch 0}])
      (is (= :inactive (:status (await (controller-runtime/await-durable! controller object-id)))))
      (finally (controller-runtime/stop! controller)))))

(deftest failed-first-audit-outbox-is-discovered-and-retried
  (let [store (memory-store)
        layout (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store layout offers {})
        object-id (identity/bytes->hex (:object-id split))]
    (try
      (await (controller-runtime/ready controller))
      (await (controller-runtime/begin! controller (sign layout0) (sign retention) (:manifest split)))
      (controller-runtime/notify-stored! controller (stored-event 0))
      (await (controller-runtime/await-durable! controller object-id))
      (reset! (:fail? store) true)
      (is (thrown-with-msg? Exception #"intent disk failed"
                            (await (controller-runtime/revalidate! controller
                                                                   (first (audit/observations (controller-runtime/state controller)))))))
      (is (= 1 (count @offers)))
      (let [repairs (repair-scheduler-runtime/start!
                     {:controller controller
                      :scheduler-opts {:scan-ms 5 :initial-grace-ms 5 :backoff-ms [5]}})]
        (try
          (await (repair-scheduler-runtime/ready repairs))
          (is (wait-for 1000 #(>= (:persistence-errors (controller-runtime/stats controller)) 2)))
          (reset! (:fail? store) false)
          (is (wait-for 1000 #(>= (count @offers) 2)))
          (is (= 1 (get-in @offers [1 :offer "attempt"])) "failed outbox retries preserve the exact fence")
          (finally (repair-scheduler-runtime/stop! repairs))))
      (finally (controller-runtime/stop! controller)))))

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
      (is (= #{authority-hex}
             (controller-runtime/known-holders controller
                                               (identity/bytes->hex
                                                (:object-id split)))))
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

(deftest bounded-scheduler-drives-the-real-durable-controller
  (let [layout-envelope (sign layout0)
        retention-envelope (sign retention)
        record (intent/make-initial-record
                layout-envelope retention-envelope (:manifest split))
        object-id (identity/bytes->hex (:object-id split))
        store (memory-store {object-id record} [])
        state (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store state offers {})
        scheduler
        (repair-scheduler-runtime/start!
         {:controller controller
          :scheduler-opts {:scan-ms 5 :initial-grace-ms 5
                           :backoff-ms [100000]
                           :max-repairs-per-scan 1
                           :max-in-flight 1}})]
    (is (true? (await (repair-scheduler-runtime/ready scheduler))))
    (is (wait-for 2000 #(= 1 (count @offers))))
    (is (= 1 (get-in @offers [0 :offer "attempt"])))
    (is (= 1 (get-in (controller-runtime/records controller)
                     [object-id "intent_revision"])))

    (await (controller-runtime/notify-stored! controller (stored-event 1)))
    (is (wait-for 2000
                  #(= #{authority-hex}
                      (get-in (controller-runtime/state controller)
                              [:objects object-id :acks]))))
    (is (repair-scheduler-runtime/request-scan! scheduler))
    (is (wait-for 2000
                  #(empty? (:entries
                            (repair-scheduler-runtime/state scheduler)))))
    (is (= 1 (get-in (repair-scheduler-runtime/state scheduler)
                     [:stats :completed])))
    (repair-scheduler-runtime/stop! scheduler)
    (controller-runtime/stop! controller)))

(deftest signed-retention-revision-persists-before-fresh-receipts
  (let [store (memory-store)
        state (atom (layout-state layout0))
        offers (atom [])
        controller (runtime store state offers {})
        layout-envelope (sign layout0)
        retention-envelope (sign retention)
        renewal (assoc retention "revision" 2
                       "issued_at" 1100 "retain_until" 6000)
        renewal-envelope (sign renewal)
        object-id (identity/bytes->hex (:object-id split))]
    (is (true? (await (controller-runtime/ready controller))))
    (is (= :ok (await (controller-runtime/begin!
                       controller layout-envelope retention-envelope
                       (:manifest split)))))
    (await (controller-runtime/notify-stored! controller (stored-event 0)))
    (is (wait-for 2000
                  #(= #{authority-hex}
                      (get-in (controller-runtime/state controller)
                              [:objects object-id :acks]))))

    (let [gate (async/chan)
          _ (reset! (:gate store) gate)
          result (controller-runtime/revise!
                  controller layout-envelope renewal-envelope)
          [_ port] (async/alts!! [result (async/timeout 50)])]
      (is (not= result port))
      (is (= 1 (count @offers)) "renewal offer escaped before persistence")
      (async/put! gate true)
      (is (= :ok (await result))))

    (is (= 2 (count @offers)))
    (is (= 1 (get-in @offers [1 :offer "attempt"])))
    (is (empty? (get-in (controller-runtime/state controller)
                        [:objects object-id :acks])))
    (is (= 2 (-> (controller-runtime/records controller)
                 (get object-id) (get "retention")
                 replication-wire/parse-envelope :statement
                 (get "revision"))))
    (is (= :exists
           (await (controller-runtime/revise!
                   controller layout-envelope renewal-envelope))))

    (await (controller-runtime/notify-stored!
            controller (stored-event renewal 1)))
    (is (wait-for 2000
                  #(= #{authority-hex}
                      (get-in (controller-runtime/state controller)
                              [:objects object-id :acks]))))
    (let [malformed (controller-runtime/revise!
                     controller {} renewal-envelope)
          [value port] (async/alts!! [malformed (async/timeout 2000)])]
      (is (= malformed port) "malformed revision did not complete")
      (is (instance? Throwable value)))
    (controller-runtime/stop! controller)))

(deftest three-replica-successor-recovery-distinguishes-quorum-from-completion
  (let [identities (mapv (fn [_] (await (identity/generate-identity))) (range 4))
        ids (mapv #(identity/authority-id-bytes (:genesis %)) identities)
        member (fn [index]
                 {"authority" (nth ids index)
                  "domains" {"owner" (str "owner-" index)} "enabled" true})
        members-for (fn [indices]
                      (vec (sort-by #(identity/bytes->hex (get % "authority"))
                                    (map member indices))))
        initial-layout (assoc layout0 "members" (members-for [0 1 2]))
        next-layout (assoc initial-layout "epoch" 1
                           "previous" (layout/address initial-layout)
                           "members" (members-for [1 2 3]))
        initial-retention (assoc retention "replicas" 3 "write_acks" 2
                                 "placement_minimum_domains" {"owner" 2}
                                 "ack_minimum_domains" {"owner" 2})
        next-retention (assoc initial-retention "revision" 2 "layout_epoch" 1)
        object-id (identity/bytes->hex (:object-id split))
        event-for
        (fn [index statement attempt]
          (let [replica (nth ids index)
                template (stored-event statement attempt)
                body (assoc (get (:receipt template) "statement")
                            "replica" replica "signer" replica)
                envelope (await (replication-wire/sign-statement
                                 (nth identities index) body))]
            (assoc template
                   :replica-id (identity/bytes->hex replica) :receipt envelope)))
        old-events (mapv #(event-for % initial-retention 0) [0 1 2])
        initial-record (intent/make-initial-record
                        (sign initial-layout) (sign initial-retention) (:manifest split))
        store (memory-store {object-id initial-record} old-events)
        current (atom (layout-state initial-layout))
        offers (atom [])
        controller (runtime store current offers {})]
    (try
      (is (true? (await (controller-runtime/ready controller))))
      (is (true? (:complete? (controller-runtime/durability-status controller object-id))))
      (let [accepted (layout/reconcile @current next-layout)]
        (is (= :accepted (:status accepted)))
        (reset! current (:state accepted)))
      (is (= :ok (await (controller-runtime/revise!
                         controller (sign next-layout) (sign next-retention)))))
      (is (= (set (map #(identity/bytes->hex (nth ids %)) [0 1 2 3]))
             (controller-runtime/known-holders controller object-id)))
      (is (= (set (map #(identity/bytes->hex (nth ids %)) [1 2 3]))
             (set (map #(identity/bytes->hex (:authority %)) @offers))))
      (is (= :under-replicated
             (:status (controller-runtime/durability-status controller object-id))))
      (finally (controller-runtime/stop! controller)))
    (let [replayed-offers (atom [])
          restarted (runtime store current replayed-offers {})]
      (try
        (is (true? (await (controller-runtime/ready restarted))))
        (is (= (set (map #(identity/bytes->hex (nth ids %)) [0 1 2 3]))
               (controller-runtime/known-holders restarted object-id)))
        (is (empty? (:acknowledged (controller-runtime/durability-status restarted object-id))))
        (is (empty? @replayed-offers))
        ;; Receipt journals can retain old evidence. Even a newly signed event
        ;; from the removed member must not become a vote for the successor.
        (await (controller-runtime/notify-stored!
                restarted (event-for 0 next-retention 1)))
        (is (empty? (:acknowledged (controller-runtime/durability-status restarted object-id))))
        (doseq [index [1 2]]
          (let [replica-id (identity/bytes->hex (nth ids index))
                attempt (get-in (controller-runtime/state restarted)
                                [:objects object-id :attempts replica-id])
                event (event-for index next-retention attempt)]
            (swap! (:receipts store) conj event)
            (await (controller-runtime/notify-stored! restarted event))))
        (is (wait-for 2000 #(= :confirmed
                               (:status (controller-runtime/durability-status restarted object-id)))))
        (is (false? (:complete? (controller-runtime/durability-status restarted object-id))))
        (is (= :ok (await (controller-runtime/repair! restarted object-id))))
        (is (= [(identity/bytes->hex (nth ids 3))]
               (mapv #(identity/bytes->hex (:authority %)) @replayed-offers)))
        (let [attempt (get-in (first @replayed-offers) [:offer "attempt"])
              event (event-for 3 next-retention attempt)]
          (swap! (:receipts store) conj event)
          (await (controller-runtime/notify-stored! restarted event)))
        (is (wait-for 2000 #(:complete? (controller-runtime/durability-status restarted object-id))))
        (finally (controller-runtime/stop! restarted))))
    (let [restored (runtime store current (atom []) {})]
      (try
        (is (true? (await (controller-runtime/ready restored))))
        (is (true? (:complete? (controller-runtime/durability-status restored object-id))))
        (finally (controller-runtime/stop! restored))))))

(deftest signed-layout-migration-replaces-the-intent-atomically
  (let [layout-envelope (sign layout0)
        retention-envelope (sign retention)
        record (intent/make-initial-record
                layout-envelope retention-envelope (:manifest split))
        object-id (identity/bytes->hex (:object-id split))
        store (memory-store {object-id record} [])
        layout1 (assoc layout0 "epoch" 1
                       "previous" (layout/address layout0)
                       "issued_at" 1000)
        layout1-envelope (sign layout1)
        migrated-retention (assoc retention
                                  "revision" 2
                                  "layout_epoch" 1
                                  "issued_at" 1100
                                  "retain_until" 6000)
        migrated-envelope (sign migrated-retention)
        layout-state1 (:state (layout/reconcile (layout-state layout0)
                                                layout1))
        state (atom layout-state1)
        offers (atom [])
        controller (runtime store state offers {})]
    (is (true? (await (controller-runtime/ready controller))))
    (is (= :ok (await (controller-runtime/revise!
                       controller layout1-envelope migrated-envelope))))
    (is (= 1 (count @offers)))
    (is (= 1 (get-in @offers [0 :offer "attempt"])))
    (is (= 1 (get-in (controller-runtime/state controller)
                     [:objects object-id :layout-epoch])))
    (is (= 1 (-> (controller-runtime/records controller)
                 (get object-id) (get "layout")
                 replication-wire/parse-envelope :statement
                 (get "epoch"))))
    (controller-runtime/stop! controller)
    ;; The layout changed without changing members or seed. A receipt for the
    ;; prior authority epoch still cannot confirm the persisted successor.
    (reset! (:receipts store) [(stored-event retention 1)])
    (let [replayed-offers (atom [])
          restarted (runtime store state replayed-offers {})]
      (try
        (is (true? (await (controller-runtime/ready restarted))))
        (is (= :under-replicated
               (:status (controller-runtime/durability-status restarted object-id))))
        (is (empty? (get-in (controller-runtime/state restarted)
                            [:objects object-id :acks])))
        (is (empty? @replayed-offers) "replay alone must not dispatch repairs")
        (is (= 1 (get-in (controller-runtime/state restarted)
                         [:objects object-id :attempts authority-hex])))
        (await (controller-runtime/notify-stored!
                restarted (stored-event migrated-retention 1)))
        (is (wait-for 2000
                      #(= :confirmed
                          (:status (controller-runtime/durability-status
                                    restarted object-id)))))
        (is (true? (:complete? (controller-runtime/durability-status
                                restarted object-id))))
        (finally (controller-runtime/stop! restarted))))))
