(ns netz.replication.controller-runtime
  "Serialized durable-outbox adapter for the pure replication controller.

  Startup loads and verifies bounded intent records, restores controller state,
  and replays durable signed receipts before readiness. Begin and explicit
  repair operations persist the exact next intent generation before dispatching
  any offer. Periodic scheduling/backoff is deliberately a later policy layer."
  (:require [netz.identity :as identity]
            [netz.replication :as replication]
            [netz.replication.controller-storage :as controller-storage]
            [netz.replication.intent :as intent]
            [netz.replication.layout :as layout]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]
            #?(:clj [clojure.core.async :as async
                     :refer [chan close! go go-loop <! >!]]
               :cljs [clojure.core.async :as async
                      :refer [chan close! <! >!]
                      :refer-macros [go go-loop]])))

(def ^:private event-capacity 256)
(def default-max-intents 100000)

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- same-bytes? [a b]
  (and (wire/byte-buffer? a) (wire/byte-buffer? b)
       (identity/bufs= a b)))

(defn- same-value? [a b]
  (same-bytes? (wire/encode-canonical a) (wire/encode-canonical b)))

(defn- result! [reply value]
  (when reply
    (async/put! reply value (fn [_] (close! reply)))))

(defn- submit! [ctx event]
  (and (not @(:stopped? ctx))
       (boolean (async/offer! (:events ctx) event))))

(defn- current-record? [ctx record]
  (try
    (let [layout-state ((:layout-state-fn ctx))
          current (get-in layout-state [:current :statement])
          recorded (:statement
                    (replication-wire/parse-envelope (get record "layout")))
          retention (:statement
                     (replication-wire/parse-envelope
                      (get record "retention")))]
      (and current
           (empty? (:conflicts layout-state))
           (same-bytes? (:cluster layout-state) (:cluster ctx))
           (same-bytes? (layout/address current) (layout/address recorded))
           (= (get current "epoch") (get retention "layout_epoch"))
           (< ((:now-fn ctx)) (get retention "retain_until"))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn- intent-record [entry]
  (let [manifest (:manifest entry)]
    (intent/make-record
     (:layout-envelope manifest)
     (:retention-envelope manifest)
     (:wire-manifest manifest)
     (:attempts entry)
     (:intent-revision entry))))

(defn- receipt-matches? [ctx event]
  (try
    (let [record (get @(:records ctx) (:object-id event))
          retention (:statement
                     (replication-wire/parse-envelope
                      (get record "retention")))
          receipt (:statement
                   (replication-wire/parse-envelope (:receipt event)))]
      (and record
           (= :replication/stored (:type event))
           (= "blob/stored" (get receipt "type"))
           (contains? (:accepted-persistence ctx) (get receipt "persistence"))
           (= (:persistence event) (get receipt "persistence"))
           (same-bytes? (get receipt "cluster") (get retention "cluster"))
           (same-bytes? (get receipt "privacy_domain")
                        (get retention "privacy_domain"))
           (= (get receipt "retention_revision") (get retention "revision"))
           (same-bytes? (get receipt "manifest") (get retention "manifest"))
           (same-bytes? (get receipt "object") (get retention "object"))
           (= (get receipt "layout_epoch") (get retention "layout_epoch"))
           (= (:layout-epoch event) (get receipt "layout_epoch"))
           (= (get receipt "attempt") (:attempt event))
           (= (get receipt "retain_until") (get retention "retain_until"))
           (= (identity/bytes->hex (get receipt "replica"))
              (:replica-id event))
           (= (:manifest-id event)
              (identity/bytes->hex (get retention "manifest")))))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn- callback! [ctx key value]
  (try
    (when-let [callback (get ctx key)] (callback value))
    (catch #?(:clj Throwable :cljs :default) _
      (swap! (:stats ctx) update :callback-errors inc))))

(declare drive! execute-actions!)

(defn- dispatch! [ctx target manifest transfer]
  (let [object-id (:object-id manifest)
        record (get @(:records ctx) object-id)
        authority (try (identity/hex->bytes target)
                       (catch #?(:clj Throwable :cljs :default) _ nil))]
    (if-not (and record authority (current-record? ctx record))
      (do
        (swap! (:stats ctx) update :dispatch-refused inc)
        (callback! ctx :on-failed!
                   {:type :replication/dispatch-failed
                    :object-id object-id :replica-id target
                    :layout-epoch (:layout-epoch transfer)
                    :attempt (:attempt transfer) :code :ineligible}))
      (let [offer {"v" 1 "type" "blob/offer"
                   "session" (identity/random-bytes 16)
                   "attempt" (:attempt transfer)
                   "replica" authority
                   "retention" (:retention-envelope manifest)
                   "manifest" (:wire-manifest manifest)}
            accepted?
            (try
              (true? ((:dispatch-offer! ctx)
                      {:replica-id target :authority authority :offer offer}))
              (catch #?(:clj Throwable :cljs :default) _ false))]
        (swap! (:stats ctx) update
               (if accepted? :dispatched :dispatch-refused) inc)
        (when-not accepted?
          (callback! ctx :on-failed!
                     {:type :replication/dispatch-failed
                      :object-id object-id :replica-id target
                      :layout-epoch (:layout-epoch transfer)
                      :attempt (:attempt transfer) :code :refused}))))))

(defn- persist-intent! [ctx object-id entry reply]
  (let [record (intent-record entry)
        claimed? (volatile! false)]
    (swap! (:persisting ctx)
           (fn [current]
             (if (contains? current object-id)
               current
               (do (vreset! claimed? true) (conj current object-id)))))
    (if-not @claimed?
      (do (result! reply :busy) true)
      (do
        (swap! (:candidates ctx) assoc object-id record)
        (go
          (let [stored
                (try
                  (<! (controller-storage/-persist-controller-intent!
                       (:store ctx) (:cluster ctx) (get record "object") record))
                  (catch #?(:clj Throwable :cljs :default) cause cause))]
            (if (= :ok stored)
              (when-not (>! (:events ctx)
                            {:kind :intent-persisted
                             :object-id object-id
                             :record record
                             :layout-epoch (:layout-epoch entry)
                             :intent-revision (:intent-revision entry)
                             :reply reply})
                (swap! (:persisting ctx) disj object-id)
                (result! reply
                         (ex-info
                          "Controller runtime stopped after persistence"
                          {:type :netz.replication/controller-stopped})))
              (do
                (swap! (:persisting ctx) disj object-id)
                (swap! (:stats ctx) update :persistence-errors inc)
                (result! reply
                         (if (failure? stored)
                           stored
                           (ex-info
                            "Controller intent persistence failed"
                            {:type
                             :netz.replication/intent-persistence-failed})))))))
        true))))

(defn- execute-action! [ctx action reply]
  (let [[op & args] action]
    (case op
      :persist-intent
      (let [[object-id entry] args]
        (persist-intent! ctx object-id entry reply))

      :store
      (let [[target manifest transfer] args]
        (dispatch! ctx target manifest transfer)
        false)

      :durable (do (callback! ctx :on-durable! (first args)) false)
      :complete (do (callback! ctx :on-complete! (first args)) false)
      :repaired (do (callback! ctx :on-repaired! (first args)) false)
      (throw (ex-info "Unknown controller runtime action"
                      {:type :netz.replication/unknown-controller-action
                       :action action})))))

(defn- execute-actions! [ctx actions reply]
  (let [persisting? (reduce (fn [result action]
                              (let [action-result
                                    (execute-action! ctx action reply)]
                                (or result action-result)))
                            false actions)]
    (when (and reply (not persisting?)) (result! reply :ok))))

(defn- drive! [ctx event reply]
  (let [{next-state :state actions :actions}
        (replication/handler @(:state ctx) event {})]
    (reset! (:state ctx) next-state)
    (execute-actions! ctx actions reply)))

(defn- restore-intents! [ctx records]
  (go
    (try
      (when-not (and (vector? records) (<= (count records) (:max-intents ctx)))
        (throw (ex-info "Invalid durable controller intent count"
                        {:type :netz.replication/too-many-controller-intents
                         :limit (:max-intents ctx)})))
      (loop [remaining (seq records) verified []]
        (if-let [record (first remaining)]
          (let [restored (<! (intent/verify-record record (:controllers ctx)))]
            (when (failure? restored) (throw restored))
            (recur (next remaining) (conj verified restored)))
          verified))
      (catch #?(:clj Throwable :cljs :default) cause cause))))

(defn- replay-receipts! [ctx receipts]
  (doseq [event receipts :when (receipt-matches? ctx event)]
    (drive! ctx event nil)))

(defn- initialize! [ctx]
  (go
    (let [loaded (try
                   (<! (controller-storage/-load-controller-intents
                        (:store ctx) (:cluster ctx)))
                   (catch #?(:clj Throwable :cljs :default) cause cause))
          intents (if (failure? loaded) loaded
                      (<! (restore-intents! ctx (or loaded []))))]
      (if (failure? intents)
        (reset! (:startup-error ctx) intents)
        (try
          (reset! (:state ctx)
                  (replication/restore-state @(:state ctx) intents))
          (reset! (:records ctx)
                  (into {} (map (fn [record]
                                  [(identity/bytes->hex (get record "object"))
                                   record]))
                        (or loaded [])))
          (let [receipts (<! (storage/-recorded-receipts (:store ctx)))]
            (if (failure? receipts)
              (reset! (:startup-error ctx) receipts)
              (replay-receipts! ctx receipts)))
          (catch #?(:clj Throwable :cljs :default) cause
            (reset! (:startup-error ctx) cause))))
      (async/put! (:ready ctx) (or @(:startup-error ctx) true))
      (close! (:ready ctx)))))

(defn- process-begin! [ctx record reply]
  (let [restored (intent/record->intent record)
        object-id (get-in restored [:manifest :object-id])
        candidate (get @(:candidates ctx) object-id)
        durable (get @(:records ctx) object-id)]
    (cond
      @(:startup-error ctx) (result! reply @(:startup-error ctx))
      (not (current-record? ctx record)) (result! reply :stale-layout)
      durable
      (result! reply (if (same-value? durable record) :exists :conflict))
      candidate
      (if (same-value? candidate record)
        (persist-intent! ctx object-id
                         (get-in @(:state ctx) [:objects object-id]) reply)
        (result! reply :conflict))
      :else
      (drive! ctx {:type :replication/begin
                   :manifest (:manifest restored)
                   :layout (:layout restored)
                   :policy (:policy restored)} reply))))

(defn- process-repair! [ctx object-id reply]
  (let [candidate (get @(:candidates ctx) object-id)
        record (or candidate (get @(:records ctx) object-id))]
    (cond
      @(:startup-error ctx) (result! reply @(:startup-error ctx))
      (nil? record) (result! reply :missing)
      (not (current-record? ctx record)) (result! reply :stale-layout)
      candidate (persist-intent! ctx object-id
                                 (get-in @(:state ctx) [:objects object-id])
                                 reply)
      :else (drive! ctx {:type :replication/repair :object-id object-id}
                    reply))))

(defn- run-loop! [ctx]
  (go-loop []
    (when-let [{:keys [kind reply] :as item} (<! (:events ctx))]
      (try
        (case kind
          :begin (process-begin! ctx (:record item) reply)
          :repair (process-repair! ctx (:object-id item) reply)
          :stored (when (receipt-matches? ctx (:event item))
                    (drive! ctx (:event item) nil))
          :failed (callback! ctx :on-failed! (:event item))
          :intent-persisted
          (do
            (swap! (:persisting ctx) disj (:object-id item))
            (swap! (:records ctx) assoc (:object-id item) (:record item))
            (swap! (:candidates ctx) dissoc (:object-id item))
            (drive! ctx {:type :replication/intent-stored
                         :object-id (:object-id item)
                         :layout-epoch (:layout-epoch item)
                         :intent-revision (:intent-revision item)}
                    reply))
          nil)
        (catch #?(:clj Throwable :cljs :default) cause
          (swap! (:stats ctx) update :event-errors inc)
          (result! reply cause)))
      (recur))))

(defn start!
  "Restore and start a durable replication controller.

  Required options are a store implementing both controller and transfer
  storage, one cluster, configured controller Authorities, a synchronous
  `:layout-state-fn`, and `:dispatch-offer!`. The dispatch callback receives
  `{replica-id authority offer}` and returns true only when the already selected
  Authority's offer entered a bounded transfer runtime. It must not choose a
  different replica. `:accepted-persistence` defaults to fsync only. Await
  `ready` before accepting application work. A failed intent write releases no
  offer; repeating the same begin or repair retries that exact generation."
  [{:keys [store cluster controllers layout-state-fn dispatch-offer!
           accepted-persistence max-intents now-fn
           on-durable! on-complete! on-repaired! on-failed!]
    :or {accepted-persistence #{"netz/store/fsync/1"}
         max-intents default-max-intents}}]
  (when-not (and (satisfies? controller-storage/PBlobControllerStore store)
                 (satisfies? storage/PBlobTransferStore store)
                 (wire/byte-buffer? cluster)
                 (<= 1 (identity/buf-length cluster) 64)
                 (seq controllers)
                 (ifn? layout-state-fn)
                 (ifn? dispatch-offer!)
                 (set? accepted-persistence)
                 (seq accepted-persistence)
                 (every? replication-wire/persistence-profiles
                         accepted-persistence)
                 (not (contains? accepted-persistence
                                 "netz/store/volatile/1"))
                 (integer? max-intents) (pos? max-intents))
    (throw (ex-info "Invalid durable controller runtime options"
                    {:type :netz.replication.controller-runtime/invalid-options})))
  (let [ctx {:store store :cluster cluster :controllers controllers
             :layout-state-fn layout-state-fn
             :dispatch-offer! dispatch-offer!
             :accepted-persistence accepted-persistence
             :max-intents max-intents
             :now-fn (or now-fn #?(:clj #(System/currentTimeMillis)
                                   :cljs #(.now js/Date)))
             :on-durable! on-durable! :on-complete! on-complete!
             :on-repaired! on-repaired! :on-failed! on-failed!
             :state (atom (replication/make-empty-state))
             :records (atom {}) :candidates (atom {})
             :persisting (atom #{})
             :events (chan event-capacity)
             :ready (async/promise-chan)
             :startup-error (atom nil) :stopped? (atom false)
             :stats (atom {:dispatched 0 :dispatch-refused 0
                           :persistence-errors 0 :callback-errors 0
                           :event-errors 0})}]
    (go
      (<! (initialize! ctx))
      (run-loop! ctx))
    ctx))

(defn ready [ctx] (:ready ctx))

(defn begin!
  "Verify and durably begin one signed retention intent.

  The result channel yields `:ok` only after the revision-zero record persists
  and its initial offers have been handed to the dispatch callback. An exact
  durable duplicate yields `:exists`; a different intent for the same object
  yields `:conflict`."
  [ctx layout-envelope retention-envelope manifest]
  (let [reply (chan 1)]
    (go
      (let [record (try
                     (intent/make-initial-record
                      layout-envelope retention-envelope manifest)
                     (catch #?(:clj Throwable :cljs :default) cause cause))
            verified (if (failure? record) record
                         (<! (intent/verify-record record (:controllers ctx))))]
        (if (failure? verified)
          (result! reply verified)
          (when-not (submit! ctx {:kind :begin :record record :reply reply})
            (result! reply :unavailable)))))
    reply))

(defn repair!
  "Persist and dispatch a fresh attempt to every currently missing target.

  If the prior outbox write failed, this retries that exact pending generation
  instead of incrementing its attempt again. Concurrent persistence yields
  `:busy`; unknown and stale-layout intents yield `:missing` and
  `:stale-layout`."
  [ctx object-id]
  (let [reply (chan 1)]
    (if (and (string? object-id) (re-matches #"[0-9a-f]{64}" object-id)
             (submit! ctx {:kind :repair :object-id object-id :reply reply}))
      reply
      (do (result! reply :unavailable) reply))))

(defn notify-stored!
  "Callback entry point for verified, durably journaled transfer receipts."
  [ctx event]
  (go
    (let [signature-ok?
          (try
            (<! (replication-wire/verify-envelope (:receipt event)))
            (catch #?(:clj Throwable :cljs :default) _ false))]
      (when (and (true? signature-ok?) (receipt-matches? ctx event))
        (submit! ctx {:kind :stored :event event})))))

(defn stored-handler [ctx] #(notify-stored! ctx %))

(defn notify-failed! [ctx event]
  (submit! ctx {:kind :failed :event event}))

(defn failed-handler [ctx] #(notify-failed! ctx %))

(defn state [ctx] @(:state ctx))
(defn records [ctx] @(:records ctx))
(defn stats [ctx] @(:stats ctx))

(defn stop! [ctx]
  (when (compare-and-set! (:stopped? ctx) false true)
    (close! (:events ctx))
    true))
