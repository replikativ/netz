(ns netz.replication.audit-runtime
  "Optional active receipt audit. Uses the existing durable repair outbox.
   An unavailable replica loses confirmation credit, never its storage authority.
   Replacement requires a controller-authorized signed layout and retention."
  (:require [netz.replication.audit :as audit]
            [netz.replication.controller-runtime :as controller]
            #?(:clj [clojure.core.async :as a :refer [go <!]]
               :cljs [clojure.core.async :as a :refer [<!] :refer-macros [go]])))

(defn start!
  [{:keys [controller audit-opts scan-ms now-fn]
    :or {scan-ms 5000 now-fn #?(:clj #(System/currentTimeMillis) :cljs #(.now js/Date))}}]
  (when-not (and controller (integer? scan-ms) (<= 1 scan-ms 2147483647) (ifn? now-fn))
    (throw (ex-info "Invalid receipt audit runtime options" {})))
  (let [ctx {:state (atom (audit/make-state audit-opts)) :stop (a/chan)
             :ready (a/promise-chan) :done (a/promise-chan)
             :stats (atom {:requested 0 :errors 0})}]
    (go
      (try
        (let [[result port] (a/alts! [(:stop ctx) (controller/ready controller)] :priority true)]
          (if (and (not= port (:stop ctx)) (true? result))
            (do
              (a/put! (:ready ctx) true)
              (loop []
                (let [[_ port] (a/alts! [(:stop ctx) (a/timeout scan-ms)] :priority true)]
                  (when-not (= port (:stop ctx))
                    (let [result (audit/scan @(:state ctx) (controller/state controller) (now-fn))]
                      (reset! (:state ctx) (:state result))
                      ;; Sequential dispatch bounds in-flight audit operations to one.
                      (doseq [observation (:revalidate result)]
                        (let [[_ stopped] (a/alts! [(:stop ctx)] :default :running)]
                          (when-not (= stopped (:stop ctx))
                            (swap! (:stats ctx) update :requested inc)
                            (let [[response _] (a/alts! [(:stop ctx)
                                                         (controller/revalidate! controller observation)]
                                                        :priority true)]
                              (when-not (= :ok response)
                                (swap! (:stats ctx) update :errors inc)))))))
                    (recur)))))
            (a/put! (:ready ctx) (ex-info "Audit controller not ready" {:result result}))))
        (catch #?(:clj Throwable :cljs :default) cause
          (swap! (:stats ctx) assoc :failure cause)
          (a/put! (:ready ctx) cause))
        (finally (a/close! (:ready ctx)) (a/put! (:done ctx) true))))
    ctx))

(defn ready [ctx] (:ready ctx))
(defn stop! [ctx] (a/close! (:stop ctx)) (:done ctx))
(defn stats [ctx] @(:stats ctx))
