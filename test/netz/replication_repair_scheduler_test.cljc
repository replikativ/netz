(ns netz.replication-repair-scheduler-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.replication.repair-scheduler :as scheduler]
            [netz.replication.repair-scheduler-runtime :as scheduler-runtime]))

(def target-a (apply str (repeat 32 "aa")))
(def target-b (apply str (repeat 32 "bb")))

(defn object
  ([object-id] (object object-id #{}))
  ([object-id acks]
   {:object-id object-id
    :layout-epoch 7
    :intent-revision 0
    :targets #{target-a target-b}
    :acks acks}))

(defn step [state event now]
  (scheduler/handler state event {:now now}))

(defn object-key [n]
  (let [suffix (str n)]
    (str (apply str (repeat (- 64 (count suffix)) "0")) suffix)))

(deftest portable-runtime-validates-its-controller-interface
  (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
               (scheduler-runtime/start! {}))))

(deftest controller-snapshot-distinguishes-durable-and-pending-evidence
  (let [objects (scheduler/controller-objects
                 {:objects
                  {"durable" {:layout-epoch 1 :intent-revision 2
                              :targets [target-a target-b]
                              :acks #{target-a} :intent-stored? true}
                   "pending" {:layout-epoch 1 :intent-revision 0
                              :targets [target-a] :acks #{}
                              :intent-stored? false}}})]
    (is (= [{:object-id "durable" :layout-epoch 1 :intent-revision 2
             :targets #{target-a target-b} :acks #{target-a}}
            {:object-id "pending" :pending? true}]
           objects))))

(deftest initial-grace-protects-attempt-zero-transfers
  (let [initial (scheduler/make-state
                 {:scan-ms 5 :initial-grace-ms 30 :backoff-ms [40]
                  :max-repairs-per-scan 2})
        first-scan (step initial {:type :repair-scheduler/scan
                                  :objects [(object "b") (object "a")]}
                         100)
        before-grace (step (:state first-scan)
                           {:type :repair-scheduler/scan
                            :objects [(object "a") (object "b")]}
                           129)
        due (step (:state before-grace)
                  {:type :repair-scheduler/scan
                   :objects [(object "a") (object "b")]}
                  130)]
    (is (= [[:timer 5 :repair-scan]] (:actions first-scan)))
    (is (= [[:timer 5 :repair-scan]] (:actions before-grace)))
    (is (= [[:repair "a"] [:repair "b"]]
           (mapv #(subvec % 0 2) (butlast (:actions due)))))
    (is (= 2 (get-in due [:state :stats :scheduled])))))

(deftest result-backoff-does-not-overlap-repairs
  (let [opts {:scan-ms 5 :initial-grace-ms 10 :backoff-ms [20 50]
              :max-repairs-per-scan 1}
        discovered (:state (step (scheduler/make-state opts)
                                 {:type :repair-scheduler/scan
                                  :objects [(object "blob")]}
                                 0))
        issued (step discovered {:type :repair-scheduler/scan
                                 :objects [(object "blob")]}
                     10)
        while-running (step (:state issued)
                            {:type :repair-scheduler/scan
                             :objects [(object "blob")]}
                            100)
        fingerprint (get-in issued [:state :entries "blob" :fingerprint])
        accepted (step (:state while-running)
                       {:type :repair-scheduler/result
                        :object-id "blob" :fingerprint fingerprint :result :ok}
                       100)
        successor (assoc (object "blob") :intent-revision 1)
        too-early (step (:state accepted)
                        {:type :repair-scheduler/scan
                         :objects [successor]}
                        119)
        retried (step (:state too-early)
                      {:type :repair-scheduler/scan
                       :objects [successor]}
                      120)]
    (is (= [:repair "blob"] (subvec (first (:actions issued)) 0 2)))
    (is (= [:timer 5 :repair-scan] (last (:actions issued))))
    (is (= [[:timer 5 :repair-scan]] (:actions while-running)))
    (is (= [[:timer 5 :repair-scan]] (:actions too-early)))
    (is (= :repair (ffirst (:actions retried))))
    (is (= 1 (get-in accepted [:state :entries "blob" :rounds])))))

(deftest late-result-cannot-mutate-a-new-intent-generation
  (let [opts {:scan-ms 5 :initial-grace-ms 1 :backoff-ms [10]
              :max-repairs-per-scan 1}
        discovered (:state (step (scheduler/make-state opts)
                                 {:type :repair-scheduler/scan
                                  :objects [(object "blob")]}
                                 0))
        issued (:state (step discovered {:type :repair-scheduler/scan
                                         :objects [(object "blob")]}
                             1))
        old-fingerprint (get-in issued [:entries "blob" :fingerprint])
        replacement (assoc (object "blob")
                           :layout-epoch 8 :intent-revision 1)
        reconciled (:state (step issued {:type :repair-scheduler/scan
                                         :objects [replacement]}
                                 2))
        late (step reconciled {:type :repair-scheduler/result
                               :object-id "blob"
                               :fingerprint old-fingerprint
                               :result :stale-layout}
                   2)]
    (is (= 8 (first (get-in late [:state :entries "blob" :fingerprint]))))
    (is (false? (get-in late [:state :entries "blob" :blocked?])))
    (is (= 3 (get-in late [:state :entries "blob" :next-at])))))

(deftest pending-outbox-and-its-durable-successor-preserve-the-operation
  (let [opts {:scan-ms 5 :initial-grace-ms 1 :backoff-ms [20]
              :max-repairs-per-scan 1}
        discovered (:state (step (scheduler/make-state opts)
                                 {:type :repair-scheduler/scan
                                  :objects [(object "blob")]}
                                 0))
        issued (:state (step discovered {:type :repair-scheduler/scan
                                         :objects [(object "blob")]}
                             1))
        operation (get-in issued [:entries "blob" :operation])
        pending (:state (step issued {:type :repair-scheduler/scan
                                      :objects [{:object-id "blob"
                                                 :pending? true}]}
                              2))
        successor (assoc (object "blob") :intent-revision 1)
        durable (:state (step pending {:type :repair-scheduler/scan
                                       :objects [successor]}
                              3))
        result (step durable {:type :repair-scheduler/result
                              :object-id "blob" :fingerprint operation
                              :result :ok}
                     3)]
    (is (= operation (get-in pending [:entries "blob" :operation])))
    (is (= operation (get-in durable [:entries "blob" :operation])))
    (is (= 1 (second (get-in durable [:entries "blob" :fingerprint]))))
    (is (false? (get-in result [:state :entries "blob" :in-flight?])))
    (is (nil? (get-in result [:state :entries "blob" :operation])))
    (is (= 1 (get-in result [:state :entries "blob" :rounds])))))

(deftest progress-completion-and-stale-layout-are-conservative
  (let [opts {:scan-ms 5 :initial-grace-ms 10 :backoff-ms [20 50]
              :max-repairs-per-scan 4}
        discovered (:state (step (scheduler/make-state opts)
                                 {:type :repair-scheduler/scan
                                  :objects [(object "blob")]}
                                 0))
        issued (:state (step discovered {:type :repair-scheduler/scan
                                         :objects [(object "blob")]}
                             10))
        fingerprint (get-in issued [:entries "blob" :fingerprint])
        accepted (:state (step issued {:type :repair-scheduler/result
                                       :object-id "blob"
                                       :fingerprint fingerprint :result :ok}
                               10))
        successor (assoc (object "blob" #{target-a}) :intent-revision 1)
        progressed (:state (step accepted {:type :repair-scheduler/scan
                                           :objects [successor]}
                                 11))
        retried (:state (step progressed {:type :repair-scheduler/scan
                                          :objects [successor]}
                              30))
        blocked (:state (step retried {:type :repair-scheduler/result
                                       :object-id "blob"
                                       :fingerprint (get-in retried
                                                            [:entries "blob"
                                                             :fingerprint])
                                       :result :stale-layout}
                              30))
        still-blocked (step blocked {:type :repair-scheduler/scan
                                     :objects [successor]}
                            100)
        changed-object (assoc (object "blob" #{target-a})
                              :layout-epoch 8 :intent-revision 2)
        changed (:state (step (:state still-blocked)
                              {:type :repair-scheduler/scan
                               :objects [changed-object]}
                              100))
        after-new-grace (step changed {:type :repair-scheduler/scan
                                       :objects [changed-object]}
                              110)
        complete (step (:state after-new-grace)
                       {:type :repair-scheduler/scan
                        :objects [(object "blob" #{target-a target-b})]}
                       111)]
    (testing "progress resets escalation without shortening the active wait"
      (is (= 0 (get-in progressed [:entries "blob" :rounds])))
      (is (= 30 (get-in progressed [:entries "blob" :next-at]))))
    (is (= [[:timer 5 :repair-scan]] (:actions still-blocked)))
    (is (= :repair (ffirst (:actions after-new-grace))))
    (is (empty? (get-in complete [:state :entries])))
    (is (= 1 (get-in complete [:state :stats :completed])))))

(deftest thousand-known-missing-objects-are-issued-in-bounded-batches
  (let [objects (mapv #(object (object-key %)) (range 1000))
        opts {:scan-ms 1 :initial-grace-ms 1 :backoff-ms [10]
              :max-repairs-per-scan 32}
        discovered (:state (step (scheduler/make-state opts)
                                 {:type :repair-scheduler/scan
                                  :objects objects}
                                 0))
        due (step discovered {:type :repair-scheduler/scan :objects objects} 1)
        repairs (filterv #(= :repair (first %)) (:actions due))
        next-scan (step (:state due)
                        {:type :repair-scheduler/scan :objects objects}
                        2)]
    (is (= 32 (count repairs)))
    (is (= (mapv #(vector :repair (object-key %)) (range 32))
           (mapv #(subvec % 0 2) repairs)))
    (is (= 1000 (get-in due [:state :stats :discovered])))
    (is (= 32 (count (filter :in-flight?
                             (vals (get-in due [:state :entries]))))))
    (is (empty? (filter #(= :repair (first %)) (:actions next-scan))))
    (is (= 32 (get-in next-scan [:state :stats :scheduled])))))
