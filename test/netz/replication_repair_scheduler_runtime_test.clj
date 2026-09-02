(ns netz.replication-repair-scheduler-runtime-test
  (:require [clojure.core.async :as async :refer [<!! chan close! put!]]
            [clojure.test :refer [deftest is]]
            [netz.replication.repair-scheduler-runtime :as runtime]))

(def object-id (apply str (repeat 64 "a")))
(def target-id (apply str (repeat 64 "b")))

(defn- ready-channel [value]
  (let [channel (async/promise-chan)]
    (put! channel value)
    (close! channel)
    channel))

(defn- wait-for [ms predicate]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (if (predicate)
        true
        (when (< (System/currentTimeMillis) deadline)
          (Thread/sleep 2)
          (recur))))))

(deftest runtime-waits-for-controller-and-repairs-known-missing-object
  (let [calls (atom [])
        controller-state
        (atom {:objects
               {object-id {:layout-epoch 1 :intent-revision 0
                           :targets #{target-id} :acks #{}
                           :intent-stored? true}}})
        repair-fn (fn [id]
                    (swap! calls conj id)
                    (let [result (chan 1)]
                      (put! result :ok)
                      (close! result)
                      result))
        scheduler
        (runtime/start!
         {:ready-channel (ready-channel true)
          :controller-state-fn #(deref controller-state)
          :repair-fn repair-fn
          :scheduler-opts {:scan-ms 5 :initial-grace-ms 5
                           :backoff-ms [1000]
                           :max-repairs-per-scan 2}})]
    (is (true? (<!! (runtime/ready scheduler))))
    (is (wait-for 500 #(= [object-id] @calls)))
    (is (wait-for 500 #(= 1 (:repair-results (runtime/stats scheduler)))))
    (swap! controller-state assoc-in [:objects object-id :acks] #{target-id})
    (is (runtime/request-scan! scheduler))
    (is (wait-for 500 #(empty? (:entries (runtime/state scheduler)))))
    (is (= 1 (get-in (runtime/state scheduler) [:stats :completed])))
    (is (runtime/stop! scheduler))
    (is (nil? (runtime/stop! scheduler)))
    (is (false? (runtime/request-scan! scheduler)))))

(deftest failed-controller-readiness-fails-closed
  (let [calls (atom 0)
        failure (ex-info "failed recovery" {})
        scheduler
        (runtime/start!
         {:ready-channel (ready-channel failure)
          :controller-state-fn (fn [] (swap! calls inc) {:objects {}})
          :repair-fn (fn [_] (swap! calls inc) nil)})]
    (is (identical? failure (<!! (runtime/ready scheduler))))
    (Thread/sleep 20)
    (is (zero? @calls))
    (is (= 1 (:startup-errors (runtime/stats scheduler))))
    (is (false? (runtime/request-scan! scheduler)))
    (runtime/stop! scheduler)))
