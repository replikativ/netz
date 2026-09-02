(ns netz.replication.lifecycle-runtime
  "Serialized operational adapter for the pure durable-blob lifecycle.

  The runtime restores its bounded journal before processing authorization
  requests. It executes journal writes inline on one event lane and answers an
  offer only after the exact retention statement is durable. Timers, deletion,
  deletion-receipt signing and replay are driven through the same lane, keeping
  crash ordering identical to `netz.replication.lifecycle`."
  (:require [netz.identity :as identity]
            [netz.replication.layout :as layout]
            [netz.replication.lifecycle :as lifecycle]
            [netz.replication.lifecycle-storage :as lifecycle-storage]
            [netz.replication.wire :as replication-wire]
            #?(:clj [clojure.core.async :as async
                     :refer [chan close! go go-loop <! >! timeout]]
               :cljs [clojure.core.async :as async
                      :refer [chan close! <! >! timeout]
                      :refer-macros [go go-loop]])))

(def ^:private event-capacity 256)

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- now [ctx]
  ((:now-fn ctx)))

(defn- submit! [ctx item]
  (and (not @(:stopped? ctx))
       (boolean (async/offer! (:events ctx) item))))

(declare drive-event!)

(defn- schedule! [ctx delay payload]
  (go
    (<! (timeout (max 0 delay)))
    (submit! ctx {:kind :event
                  :event {:type :timer :payload payload}})))

(defn- execute-action! [ctx [op & args :as action]]
  (go
    (try
      (case op
        :persist
        (let [[key value payload] args
              result (<! (lifecycle-storage/-persist-lifecycle!
                          (:store ctx) key value))]
          {:type :persist-result :payload payload
           :result result})

        :timer
        (let [[delay payload] args]
          (schedule! ctx delay payload)
          nil)

        :delete-object
        (let [[key statement persistence] args
              result (<! (lifecycle-storage/-delete-object!
                          (:store ctx) key statement persistence))]
          (if (and (map? result) (= :ok (:result result)))
            (merge {:type :lifecycle/delete-result :key key} result)
            {:type :lifecycle/delete-result :key key
             :result (if (failure? result) result :error)}))

        :sign-deletion-receipt
        (let [[key statement] args
              envelope (<! (replication-wire/sign-statement
                            (:identity ctx) statement))]
          (if (failure? envelope)
            (do
              (swap! (:stats ctx) update :signing-errors inc)
              nil)
            {:type :lifecycle/receipt-signed
             :key key :envelope envelope}))

        :emit-deletion-receipt
        (let [[key envelope] args]
          (try
            (when-let [callback (:on-deletion-receipt! ctx)]
              (callback {:key key :envelope envelope}))
            (swap! (:stats ctx) update :deletion-receipts inc)
            (catch #?(:clj Throwable :cljs :default) _
              (swap! (:stats ctx) update :callback-errors inc)))
          nil)

        (throw (ex-info "Unknown blob lifecycle runtime action"
                        {:type :netz.replication.lifecycle-runtime/unknown-action
                         :action action})))
      (catch #?(:clj Throwable :cljs :default) cause
        (swap! (:stats ctx) update :action-errors inc)
        (case op
          :persist {:type :persist-result :payload (nth args 2 nil)
                    :result cause}
          :delete-object {:type :lifecycle/delete-result
                          :key (first args) :result cause}
          nil)))))

(defn- drive-actions! [ctx actions]
  (go
    (loop [remaining (seq actions)]
      (when-let [action (first remaining)]
        (when-let [completion (<! (execute-action! ctx action))]
          (<! (drive-event! ctx completion)))
        (recur (next remaining))))))

(defn- drive-event! [ctx event]
  (go
    (let [{next-state :state actions :actions}
          (lifecycle/handler @(:state ctx) event {:now (now ctx)})]
      (reset! (:state ctx) next-state)
      (<! (drive-actions! ctx actions))
      @(:state ctx))))

(defn- request-admitted? [ctx request]
  (try
    (let [frame (:frame request)
          local (get @(:state ctx) :local-authority)]
      (and (map? request)
           (map? frame)
           (identity/bufs= local (get frame "replica"))
           (if-let [layout-state-fn (:layout-state-fn ctx)]
             (layout/retention-authorized? (layout-state-fn)
                                           local
                                           (:retention request))
             true)
           (if-let [admit? (:admit-offer? ctx)]
             (true? (admit? request))
             true)))
    (catch #?(:clj Throwable :cljs :default) _ false)))

(defn- initialize! [ctx]
  (go
    (let [result
          (try
            (let [records (<! (lifecycle-storage/-load-lifecycle-records
                               (:store ctx)))]
              (if (failure? records)
                records
                (let [receipt-valid?
                      (loop [remaining (seq (vals records))]
                        (if-let [record (first remaining)]
                          (if-let [envelope (:receipt-envelope record)]
                            (if (true? (<! (replication-wire/verify-envelope
                                            envelope)))
                              (recur (next remaining))
                              false)
                            (recur (next remaining)))
                          true))]
                  (if-not receipt-valid?
                    (ex-info
                     "Lifecycle journal contains an invalid deletion receipt"
                     {:type :netz.replication/invalid-lifecycle-receipt})
                    (do
                      (swap! (:state ctx) lifecycle/restore-state records)
                      (<! (drive-event! ctx {:type :init}))
                      true)))))
            (catch #?(:clj Throwable :cljs :default) cause cause))]
      (if (failure? result)
        (do
          (reset! (:startup-error ctx) result)
          (swap! (:stats ctx) update :startup-errors inc))
        nil)
      (async/put! (:ready ctx) (or @(:startup-error ctx) true))
      (close! (:ready ctx)))))

(defn- run-loop! [ctx]
  (go-loop []
    (when-let [{:keys [kind event request reply]} (<! (:events ctx))]
      (case kind
        :event (<! (drive-event! ctx event))

        :authorize
        (let [accepted?
              (if (or @(:startup-error ctx)
                      (not (request-admitted? ctx request)))
                false
                (do
                  (<! (drive-event!
                       ctx {:type :lifecycle/verified-statement
                            :statement (:retention request)}))
                  (lifecycle/accepted-retention?
                   @(:state ctx) (:retention request))))]
          (swap! (:stats ctx) update
                 (if accepted? :authorized :rejected) inc)
          (>! reply accepted?)
          (close! reply))

        :statement
        (let [statement (:statement request)]
          (when-not @(:startup-error ctx)
            (<! (drive-event! ctx {:type :lifecycle/verified-statement
                                   :statement statement})))
          (let [accepted? (and (nil? @(:startup-error ctx))
                               (lifecycle/accepted-statement?
                                @(:state ctx) statement))]
            (swap! (:stats ctx) update
                   (if accepted? :statements-accepted :rejected) inc)
            (>! reply accepted?)
            (close! reply)))
        nil)
      (recur))))

(defn start!
  "Start a lifecycle policy service.

  Required options are `:identity`, a `PBlobLifecycleStore`, and the configured
  controller Authority IDs. `:lifecycle-opts` are passed to the pure machine.
  An optional synchronous `:layout-state-fn` returns the current reconciled
  signed-layout state and fails closed unless this replica is a deterministic
  target. `:admit-offer?` may add further deployment policy before journal IO;
  neither callback may perform blocking work. `:on-deletion-receipt!`
  receives replayed and newly signed durable deletion receipts.

  Call `ready` to observe journal restoration. `offer-authorizer` produces the
  callback expected by `netz.replication.runtime/start!`."
  [{:keys [identity store controllers lifecycle-opts now-fn
           layout-state-fn admit-offer? on-deletion-receipt!]}]
  (when-not (and identity
                 (satisfies? lifecycle-storage/PBlobLifecycleStore store)
                 (seq controllers))
    (throw (ex-info "Invalid blob lifecycle runtime options"
                    {:type :netz.replication.lifecycle-runtime/invalid-options})))
  (let [authority (identity/authority-id-bytes (:genesis identity))
        ctx {:identity identity
             :store store
             :state (atom (lifecycle/make-state authority controllers
                                                lifecycle-opts))
             :now-fn (or now-fn #?(:clj #(System/currentTimeMillis)
                                   :cljs #(.now js/Date)))
             :layout-state-fn layout-state-fn
             :admit-offer? admit-offer?
             :on-deletion-receipt! on-deletion-receipt!
             :events (chan event-capacity)
             :ready (async/promise-chan)
             :startup-error (atom nil)
             :stopped? (atom false)
             :stats (atom {:authorized 0 :rejected 0 :startup-errors 0
                           :action-errors 0 :signing-errors 0
                           :statements-accepted 0
                           :deletion-receipts 0 :callback-errors 0})}]
    ;; Recovery owns the lane before queued offers can be observed. Calls to
    ;; `authorize-offer!` may enqueue immediately, but the loop starts only
    ;; after the durable journal has either restored or failed closed.
    (go
      (<! (initialize! ctx))
      (run-loop! ctx))
    ctx))

(defn ready
  "Channel yielding true after recovery, or the startup failure value."
  [ctx]
  (:ready ctx))

(defn authorize-offer!
  "Submit one already signature-verified offer request.

  The result channel yields true only after the exact retention statement is
  durably active. Queue overflow and stopped runtimes fail closed."
  [ctx request]
  (let [reply (chan 1)]
    (if (submit! ctx {:kind :authorize :request request :reply reply})
      reply
      (do (async/put! reply false) (close! reply) reply))))

(defn offer-authorizer
  "Return the callback shape consumed by `netz.replication.runtime/start!`."
  [ctx]
  #(authorize-offer! ctx %))

(defn apply-envelope!
  "Verify and durably apply a signed retention, lease or deletion envelope.

  This is the control-plane entry point for reconciled/pub-sub lifecycle
  records. The result yields true only when the exact statement is in the
  durable journal. Invalid signatures, queue overflow, startup failure and a
  stopped runtime yield false."
  [ctx envelope]
  (let [reply (chan 1)]
    (go
      (let [verified? (try
                        (<! (replication-wire/verify-envelope envelope))
                        (catch #?(:clj Throwable :cljs :default) _ false))]
        (if-not (true? verified?)
          (do
            (swap! (:stats ctx) update :rejected inc)
            (>! reply false)
            (close! reply))
          (let [statement
                (try
                  (:statement (replication-wire/parse-envelope envelope))
                  (catch #?(:clj Throwable :cljs :default) _ nil))]
            (if (and statement
                     (submit! ctx {:kind :statement
                                   :request {:statement statement}
                                   :reply reply}))
              nil
              (do (>! reply false) (close! reply)))))))
    reply))

(defn state [ctx] @(:state ctx))
(defn stats [ctx] @(:stats ctx))

(defn stop!
  "Stop accepting lifecycle work. Idempotent."
  [ctx]
  (when (compare-and-set! (:stopped? ctx) false true)
    (close! (:events ctx))
    true))
