(ns netz.replication.layout-runtime
  "Serialized verify, persist and activation boundary for signed layouts."
  (:require [netz.replication.layout :as layout]
            [netz.replication.layout-storage :as layout-storage]
            [netz.replication.wire :as replication-wire]
            #?(:clj [clojure.core.async :as async
                     :refer [chan close! go go-loop <! >!]]
               :cljs [clojure.core.async :as async
                      :refer [chan close! <! >!]
                      :refer-macros [go go-loop]])))

(def ^:private event-capacity 64)
(def default-max-records 256)

(defn- failure? [value]
  (instance? #?(:clj Throwable :cljs js/Error) value))

(defn- layout-statement [envelope]
  (try
    (let [statement (:statement (replication-wire/parse-envelope envelope))]
      (when (= layout/statement-type (get statement "type")) statement))
    (catch #?(:clj Throwable :cljs :default) _ nil)))

(defn- replay! [initial envelopes max-records]
  (go
    (try
      (when-not (and (vector? envelopes) (<= (count envelopes) max-records))
        (throw (ex-info "Invalid durable layout transcript size"
                        {:type :netz.replication/invalid-layout-transcript
                         :limit max-records})))
      (loop [state initial remaining (seq envelopes)]
        (if-let [envelope (first remaining)]
          (let [verified? (<! (replication-wire/verify-envelope envelope))
                statement (when (true? verified?) (layout-statement envelope))
                result (when statement (layout/reconcile state statement))
                status (:status result)]
            (when-not (contains? #{:accepted :conflict} status)
              (throw (ex-info "Invalid durable layout transcript chain"
                              {:type :netz.replication/invalid-layout-transcript
                               :status status})))
            (recur (:state result) (next remaining)))
          state))
      (catch #?(:clj Throwable :cljs :default) cause cause))))

(defn- submit! [ctx envelope reply]
  (and (not @(:stopped? ctx))
       (boolean (async/offer! (:events ctx)
                              {:envelope envelope :reply reply}))))

(defn- apply-one! [ctx envelope]
  (go
    (try
      (let [verified? (<! (replication-wire/verify-envelope envelope))
            statement (when (true? verified?) (layout-statement envelope))
            result (when statement (layout/reconcile @(:state ctx) statement))
            status (or (:status result) :rejected)]
        (if-not (contains? #{:accepted :conflict} status)
          status
          (let [next-transcript (conj @(:transcript ctx) envelope)]
            (if (> (count next-transcript) (:max-records ctx))
              :full
              (let [stored (<! (layout-storage/-persist-layout-transcript!
                                (:store ctx) (:cluster ctx) next-transcript))]
                (if (= :ok stored)
                  (do
                    (reset! (:transcript ctx) next-transcript)
                    (reset! (:state ctx) (:state result))
                    status)
                  (do
                    ;; A witnessed valid fork must stop in-process admission
                    ;; even when its evidence cannot reach durable storage.
                    (when (= :conflict status)
                      (reset! (:state ctx) (:state result)))
                    :persistence-failed)))))))
      (catch #?(:clj Throwable :cljs :default) _ :rejected))))

(defn- initialize! [ctx]
  (go
    (let [loaded (try
                   (<! (layout-storage/-load-layout-transcript
                        (:store ctx) (:cluster ctx)))
                   (catch #?(:clj Throwable :cljs :default) cause cause))
          result (if (failure? loaded)
                   loaded
                   (<! (replay! @(:state ctx) (or loaded [])
                                (:max-records ctx))))]
      (if (failure? result)
        (reset! (:startup-error ctx) result)
        (do
          (reset! (:state ctx) result)
          (reset! (:transcript ctx) (or loaded []))))
      (async/put! (:ready ctx) (or @(:startup-error ctx) true))
      (close! (:ready ctx)))))

(defn- run-loop! [ctx]
  (go-loop []
    (when-let [{:keys [envelope reply]} (<! (:events ctx))]
      (let [status (if @(:startup-error ctx)
                     :unavailable
                     (<! (apply-one! ctx envelope)))]
        (swap! (:stats ctx) update status (fnil inc 0))
        (>! reply status)
        (close! reply))
      (recur))))

(defn start!
  "Restore and start one durable signed-layout service.

  The store is read before the event lane starts. A newly verified envelope is
  activated only after the complete enlarged transcript persists."
  [{:keys [store cluster controllers max-records]
    :or {max-records default-max-records}}]
  (when-not (and (satisfies? layout-storage/PBlobLayoutStore store)
                 (integer? max-records) (pos? max-records))
    (throw (ex-info "Invalid layout runtime options"
                    {:type :netz.replication.layout-runtime/invalid-options})))
  (let [ctx {:store store
             :cluster cluster
             :state (atom (layout/make-state cluster controllers))
             :transcript (atom [])
             :max-records max-records
             :events (chan event-capacity)
             :ready (async/promise-chan)
             :startup-error (atom nil)
             :stopped? (atom false)
             :stats (atom {})}]
    (go
      (<! (initialize! ctx))
      (run-loop! ctx))
    ctx))

(defn ready [ctx] (:ready ctx))

(defn apply-envelope!
  "Verify and durably reconcile one signed layout envelope.

  The result is `:accepted`, `:duplicate`, `:conflict`, `:stale`, `:gap`,
  `:forked`, `:rejected`, `:full`, `:persistence-failed`, or `:unavailable`."
  [ctx envelope]
  (let [reply (chan 1)]
    (if (submit! ctx envelope reply)
      reply
      (do (async/put! reply :unavailable) (close! reply) reply))))

(defn state [ctx] @(:state ctx))
(defn transcript [ctx] @(:transcript ctx))
(defn stats [ctx] @(:stats ctx))

(defn stop! [ctx]
  (when (compare-and-set! (:stopped? ctx) false true)
    (close! (:events ctx))
    true))
