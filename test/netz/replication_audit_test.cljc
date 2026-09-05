(ns netz.replication-audit-test
  (:require [netz.replication.audit :as audit]
            [netz.replication.audit-runtime :as runtime]
            #?(:clj [clojure.test :refer [deftest is]]
               :cljs [cljs.test :refer-macros [deftest is]])))

(def controller
  {:objects {"object" {:intent-stored? true :layout-epoch 0 :intent-revision 0
                       :acks #{"a" "b"} :attempts {"a" 0 "b" 0}}}})

(deftest runtime-boundary-compiles-on-both-hosts
  (is (fn? runtime/start!))
  (is (fn? runtime/stop!)))

(deftest audits-rotate-without-starving-other-targets
  (let [initial (audit/scan (audit/make-state {:interval-ms 10}) controller 0)
        first-pass (audit/scan (:state initial) controller 10)
        successor (assoc-in controller [:objects "object" :intent-revision] 1)
        second-pass (audit/scan (:state first-pass) successor 11)]
    (is (empty? (:revalidate initial)))
    (is (= ["a"] (mapv :replica-id (:revalidate first-pass))))
    (is (= ["b"] (mapv :replica-id (:revalidate second-pass))))
    (is (= [1] (mapv :intent-revision (:revalidate second-pass))))
    (is (empty? (:revalidate (audit/scan (:state second-pass) successor 12))))))

(deftest new-receipt-starts-new-grace-period-and-missing-replicas-use-repair
  (let [initial (:state (audit/scan (audit/make-state {:interval-ms 10}) controller 0))
        new-receipt (assoc-in controller [:objects "object" :attempts "a"] 1)
        result (audit/scan initial new-receipt 10)
        missing (assoc-in new-receipt [:objects "object" :acks] #{})]
    (is (= ["b"] (mapv :replica-id (:revalidate result))))
    (is (empty? (:entries (:state (audit/scan (:state result) missing 100)))))))

(deftest capacity-and-pending-intents-fail-closed
  (is (thrown? #?(:clj Exception :cljs js/Error)
               (audit/scan (audit/make-state {:max-entries 1 :max-per-scan 1}) controller 0)))
  (is (empty? (:revalidate
               (audit/scan (audit/make-state)
                           (assoc-in controller [:objects "object" :intent-stored?] false) 999999)))))

(deftest pending-persistence-does-not-reset-other-targets-grace
  (let [initial (:state (audit/scan (audit/make-state {:interval-ms 10}) controller 0))
        pending (:state (audit/scan initial
                                    (assoc-in controller [:objects "object" :intent-stored?] false) 5))]
    (is (= (:entries initial) (:entries pending)))
    (is (= 1 (count (:revalidate (audit/scan pending controller 10)))))))

(deftest bounded-many-object-audits-eventually-visit-every-replica
  (let [objects (into {} (for [n (range 120)]
                           [(str "object-" n)
                            {:intent-stored? true :layout-epoch 0 :intent-revision 0
                             :acks #{"a" "b" "c"} :attempts {"a" 0 "b" 0 "c" 0}}]))
        initial (audit/make-state {:interval-ms 5 :max-per-scan 16})
        visited
        (loop [tick 0 schedule initial controller {:objects objects} visited #{}]
          (if (= tick 100) visited
              (let [{:keys [state revalidate]} (audit/scan schedule controller tick)
                    next-controller
                    (reduce (fn [c {:keys [object-id replica-id]}]
                              ;; Model a durable fresh receipt after each audit.
                              (-> c
                                  (update-in [:objects object-id :intent-revision] inc)
                                  (update-in [:objects object-id :attempts replica-id] inc)))
                            controller revalidate)]
                (is (<= (count revalidate) 16))
                (recur (inc tick) state next-controller
                       (into visited (map (juxt :object-id :replica-id)) revalidate)))))]
    (is (= 360 (count visited)))))
