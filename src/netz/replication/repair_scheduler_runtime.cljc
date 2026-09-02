(ns netz.replication.repair-scheduler-runtime
  "Portable timer adapter for the pure known-missing repair scheduler.

  The durable controller remains the only owner of intent generations and
  offer dispatch. This runtime merely snapshots its durable evidence, invokes
  explicit repair, and feeds results back into one serialized policy lane."
  (:require [netz.replication.controller-runtime :as controller-runtime]
            [netz.replication.repair-scheduler :as scheduler]
            #?(:clj [clojure.core.async :as async
                     :refer [chan close! go go-loop <! >! timeout]]
               :cljs [clojure.core.async :as async
                      :refer [chan close! <! >! timeout]
                      :refer-macros [go go-loop]])))

(def ^:private event-capacity 256)

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- put-ready! [ctx value]
  (async/put! (:ready ctx) value (fn [_] (close! (:ready ctx)))))

(defn- submit! [ctx item]
  (and @(:operational? ctx)
       (not @(:stopped? ctx))
       (boolean (async/offer! (:events ctx) item))))

(defn- schedule! [ctx delay]
  (let [generation (swap! (:timer-generation ctx) inc)]
    (go
      (<! (timeout (max 0 delay)))
      (when (and (not @(:stopped? ctx))
                 (= generation @(:timer-generation ctx)))
        (>! (:events ctx) {:kind :scan}))))
  nil)

(defn- run-repair! [ctx object-id fingerprint]
  (go
    (let [result
          (try
            (if-let [result-channel ((:repair-fn ctx) object-id)]
              (<! result-channel)
              (ex-info "Repair callback returned no result channel"
                       {:type :netz.replication/missing-repair-result-channel
                        :object-id object-id}))
            (catch #?(:clj Throwable :cljs :default) cause cause))]
      ;; A blocking put is deliberate: dropping this result would leave the
      ;; pure entry in flight forever. Closing the runtime releases the put.
      (when (>! (:events ctx) {:kind :result
                               :object-id object-id
                               :fingerprint fingerprint
                               :result result})
        (swap! (:runtime-stats ctx) update :repair-results inc))))
  nil)

(defn- execute-actions! [ctx actions]
  (doseq [[op & args :as action] actions]
    (case op
      :timer (schedule! ctx (first args))
      :repair (run-repair! ctx (first args) (second args))
      (throw (ex-info "Unknown repair scheduler runtime action"
                      {:type :netz.replication/unknown-repair-runtime-action
                       :action action}))))
  nil)

(defn- drive! [ctx event]
  (let [{next-state :state actions :actions}
        (scheduler/handler @(:state ctx) event {:now ((:now-fn ctx))})]
    (reset! (:state ctx) next-state)
    (execute-actions! ctx actions)))

(defn- scan! [ctx]
  (try
    (drive! ctx {:type :repair-scheduler/scan
                 :objects (scheduler/controller-objects
                           ((:controller-state-fn ctx)))})
    (catch #?(:clj Throwable :cljs :default) _
      (swap! (:runtime-stats ctx) update :snapshot-errors inc)
      (schedule! ctx (get-in @(:state ctx) [:opts :scan-ms])))))

(defn- run-loop! [ctx]
  (go-loop []
    (when-let [{:keys [kind object-id fingerprint result]} (<! (:events ctx))]
      (try
        (case kind
          :scan (scan! ctx)
          :result (drive! ctx {:type :repair-scheduler/result
                               :object-id object-id
                               :fingerprint fingerprint :result result})
          nil)
        (catch #?(:clj Throwable :cljs :default) _
          (swap! (:runtime-stats ctx) update :event-errors inc)))
      (recur))))

(defn start!
  "Start bounded automatic repair of controller-known missing replicas.

  Pass a durable controller from `controller-runtime/start!`. Await `ready`
  before treating the scheduler as operational. `:scheduler-opts` configure
  scan cadence, initial grace, retry backoff, and per-scan/global issue bounds.

  The optional `:ready-channel`, `:controller-state-fn`, and `:repair-fn`
  adapter hooks make the policy usable with another implementation of the same
  controller interface; all three must resolve when `:controller` is absent."
  [{:keys [controller scheduler-opts now-fn ready-channel
           controller-state-fn repair-fn]}]
  (let [ready-channel (or ready-channel
                          (when controller (controller-runtime/ready controller)))
        controller-state-fn
        (or controller-state-fn
            (when controller #(controller-runtime/state controller)))
        repair-fn
        (or repair-fn
            (when controller #(controller-runtime/repair! controller %)))]
    (when-not (and ready-channel (ifn? controller-state-fn) (ifn? repair-fn))
      (throw (ex-info "Invalid repair scheduler runtime options"
                      {:type :netz.replication.repair-scheduler-runtime/invalid-options})))
    (let [ctx {:state (atom (scheduler/make-state scheduler-opts))
               :now-fn (or now-fn #?(:clj #(System/currentTimeMillis)
                                     :cljs #(.now js/Date)))
               :controller-state-fn controller-state-fn
               :repair-fn repair-fn
               :events (chan event-capacity)
               :ready (async/promise-chan)
               :operational? (atom false)
               :stopped? (atom false)
               :timer-generation (atom 0)
               :runtime-stats (atom {:startup-errors 0 :snapshot-errors 0
                                     :event-errors 0 :repair-results 0})}]
      (go
        (let [controller-ready (<! ready-channel)]
          (if (true? controller-ready)
            (if @(:stopped? ctx)
              (put-ready! ctx
                          (ex-info "Repair scheduler stopped before readiness"
                                   {:type :netz.replication/scheduler-stopped}))
              (do
                (drive! ctx {:type :repair-scheduler/init})
                (run-loop! ctx)
                (reset! (:operational? ctx) true)
                (put-ready! ctx true)))
            (let [failure (if (failure? controller-ready)
                            controller-ready
                            (ex-info "Controller did not become ready"
                                     {:type :netz.replication/controller-not-ready}))]
              (swap! (:runtime-stats ctx) update :startup-errors inc)
              (put-ready! ctx failure)))))
      ctx)))

(defn ready
  "Channel yielding true after startup, or the controller readiness failure."
  [ctx]
  (:ready ctx))

(defn request-scan!
  "Request a best-effort immediate scan; returns false on overflow or stop."
  [ctx]
  (submit! ctx {:kind :scan}))

(defn state [ctx] @(:state ctx))
(defn stats [ctx] @(:runtime-stats ctx))

(defn stop!
  "Stop scheduling and accepting repair work. Idempotent."
  [ctx]
  (when (compare-and-set! (:stopped? ctx) false true)
    (reset! (:operational? ctx) false)
    (swap! (:timer-generation ctx) inc)
    (close! (:events ctx))
    true))
