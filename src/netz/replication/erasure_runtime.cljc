(ns netz.replication.erasure-runtime
  "Serialized durable runtime for controller-side erasure campaigns.

  Network delivery is an injected, peer-targeted callback. Failed or offline
  deliveries remain pending and are retried; only verified deletion receipts
  change completion evidence."
  (:require [netz.replication.erasure :as erasure]
            [netz.replication.erasure-storage :as erasure-storage]
            [netz.replication.wire :as replication-wire]
            #?(:clj [clojure.core.async :as async
                     :refer [chan close! go go-loop <! >! timeout]]
               :cljs [clojure.core.async :as async
                      :refer [chan close! <! >! timeout]
                      :refer-macros [go go-loop]])))

(def ^:private event-capacity 256)

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- result! [reply value]
  (when reply
    (async/put! reply value (fn [_] (close! reply)))))

(defn- submit! [ctx event]
  (and (not @(:stopped? ctx))
       (boolean (async/offer! (:events ctx) event))))

(defn- callback! [ctx key value]
  (try
    (when-let [callback (get ctx key)] (callback value))
    (catch #?(:clj Throwable :cljs :default) _
      (swap! (:stats ctx) update :callback-errors inc))))

(declare drive!)

(defn- execute-action! [ctx [op & args]]
  (case op
    :persist-erasure
    (let [[key record] args]
      (go
        (let [result
              (try
                (<! (erasure-storage/-persist-erasure-record!
                     (:store ctx) (:cluster ctx) key record))
                (catch #?(:clj Throwable :cljs :default) cause cause))]
          (<! (drive! ctx {:type :erasure/persisted :key key
                           :result (if (= :ok result) :ok result)} nil)))))

    :dispatch-deletion
    (let [[replica-id envelope] args
          accepted? (try
                      (true? ((:dispatch-deletion! ctx)
                              {:replica-id replica-id :envelope envelope}))
                      (catch #?(:clj Throwable :cljs :default) _ false))]
      (swap! (:stats ctx) update (if accepted? :dispatched :refused) inc)
      nil)

    :erasure-progress
    (let [[key] args]
      (callback! ctx :on-progress! (erasure/status @(:state ctx) key))
      nil)

    (throw (ex-info "Unknown erasure runtime action"
                    {:type ::unknown-action :operation op}))))

(defn- drive! [ctx event reply]
  (go
    (let [before @(:state ctx)
          {next-state :state actions :actions}
          (erasure/handler @(:state ctx) event {:now ((:now-fn ctx))})]
      (reset! (:state ctx) next-state)
      (loop [remaining (seq actions)]
        (when-let [action (first remaining)]
          (when-let [pending (execute-action! ctx action)] (<! pending))
          (recur (next remaining))))
      (result! reply
               (and (= (get-in before [:stats :rejected])
                       (get-in @(:state ctx) [:stats :rejected]))
                    (= (get-in before [:stats :persistence-failures])
                       (get-in @(:state ctx) [:stats :persistence-failures]))))
      next-state)))

(defn- verify-records! [records]
  (go
    (loop [remaining (seq (vals records))]
      (if-let [record (first remaining)]
        (let [envelopes (cons (:deletion-envelope record)
                              (vals (:receipts record)))]
          (if (loop [pending (seq envelopes)]
                (if-let [envelope (first pending)]
                  (if (true? (<! (replication-wire/verify-envelope envelope)))
                    (recur (next pending))
                    false)
                  true))
            (recur (next remaining))
            false))
        true))))

(defn- initialize! [ctx]
  (go
    (let [result
          (try
            (let [records (<! (erasure-storage/-load-erasure-records
                               (:store ctx) (:cluster ctx)))]
              (cond
                (failure? records) records
                (not (true? (<! (verify-records! records))))
                (ex-info "Erasure journal contains an invalid signature"
                         {:type ::invalid-signature})
                :else
                (do (swap! (:state ctx) erasure/restore-state records)
                    (<! (drive! ctx {:type :init} nil))
                    true)))
            (catch #?(:clj Throwable :cljs :default) cause cause))]
      (when (failure? result)
        (reset! (:startup-error ctx) result)
        (swap! (:stats ctx) update :startup-errors inc))
      (async/put! (:ready ctx) (or @(:startup-error ctx) true))
      (close! (:ready ctx)))))

(defn- run-loop! [ctx]
  (go-loop []
    (when-let [{:keys [event reply]} (<! (:events ctx))]
      (if @(:startup-error ctx)
        (result! reply @(:startup-error ctx))
        (<! (drive! ctx event reply)))
      (recur))))

(defn- start-ticker! [ctx]
  (go-loop []
    (<! (timeout (:scan-ms ctx)))
    (when (submit! ctx {:event {:type :erasure/tick}})
      (recur))))

(defn start!
  "Start an erasure coordinator.

  `dispatch-deletion!` receives `{:replica-id hex :envelope signed-deletion}`
  and returns true when the targeted carrier accepted the delivery attempt.
  This is transport evidence only. `on-progress!` receives durable receipt
  progress maps. Call `ready` before beginning campaigns."
  [{:keys [cluster controllers store dispatch-deletion! erasure-opts
           scan-ms now-fn on-progress!]
    :or {scan-ms 5000
         now-fn #?(:clj #(System/currentTimeMillis) :cljs #(.now js/Date))}}]
  (when-not (and cluster (seq controllers)
                 (satisfies? erasure-storage/PBlobErasureStore store)
                 (ifn? dispatch-deletion!) (ifn? now-fn)
                 (integer? scan-ms) (<= 1 scan-ms 2147483647))
    (throw (ex-info "Invalid erasure runtime options" {:type ::invalid-options})))
  (let [ctx {:cluster cluster :store store
             :dispatch-deletion! dispatch-deletion!
             :on-progress! on-progress!
             :state (atom (erasure/make-state cluster controllers erasure-opts))
             :now-fn now-fn :scan-ms scan-ms
             :events (chan event-capacity) :ready (async/promise-chan)
             :startup-error (atom nil) :stopped? (atom false)
             :stats (atom {:dispatched 0 :refused 0 :callback-errors 0
                           :startup-errors 0 :verification-failures 0})}]
    (go
      (<! (initialize! ctx))
      (run-loop! ctx)
      (start-ticker! ctx))
    ctx))

(defn ready [ctx] (:ready ctx))
(defn state [ctx] @(:state ctx))
(defn stats [ctx] @(:stats ctx))
(defn status [ctx key] (erasure/status @(:state ctx) key))

(defn- verified-submit! [ctx envelope event]
  (let [reply (chan 1)]
    (go
      (if (true? (try (<! (replication-wire/verify-envelope envelope))
                      (catch #?(:clj Throwable :cljs :default) _ false)))
        (when-not (submit! ctx {:event event :reply reply})
          (result! reply false))
        (do (swap! (:stats ctx) update :verification-failures inc)
            (result! reply false))))
    reply))

(defn begin!
  "Durably begin deletion for the explicit set of all known holders."
  [ctx deletion-envelope holders]
  (verified-submit! ctx deletion-envelope
                    {:type :erasure/begin :deletion-envelope deletion-envelope
                     :holders holders}))

(defn accept-receipt!
  "Verify and durably record one replica's signed deletion receipt."
  [ctx receipt-envelope]
  (verified-submit! ctx receipt-envelope
                    {:type :erasure/receipt :envelope receipt-envelope}))

(defn stop! [ctx]
  (when (compare-and-set! (:stopped? ctx) false true)
    (close! (:events ctx))
    true))
