(ns netz.apply-owner-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.apply-owner :as owner]))

(def generation :g1)

(defn state
  ([] (state {}))
  ([opts]
   (owner/make-state {:topic :doc :profile "join/1" :generation generation
                      :phase :live :live-transport :overlay :opts opts})))

(defn live
  ([origin seq-no] (live origin seq-no {}))
  ([origin seq-no extra]
   (merge {:type :live :generation generation :transport :overlay
           :source :peer :receipt [origin seq-no]
           :publication {:origin origin :epoch 0 :topic :doc :seq seq-no
                         :payload [origin seq-no] :bytes 10}}
          extra)))

(defn step [state event]
  (owner/transition state event))

(deftest live-transport-is-exclusive
  (let [{:keys [state effects]}
        (step (state) (assoc (live :a 0) :transport :direct))]
    (is (nil? (:in-flight state)))
    (is (= [:settle :retire-direct] (mapv :op effects)))
    (is (= :netz/wrong-live-transport (get-in effects [0 :result :error])))))

(deftest a-frontier-advances-only-after-literal-apply-success
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        token (:token (first e1))]
    (is (= :invoke-live (:op (first e1))))
    (is (empty? (:applied-frontiers s1)) "admission is not application")
    (let [{s2 :state e2 :effects}
          (step s1 {:type :apply-result :token token :result {:ok true}})]
      (is (= 0 (get-in s2 [:applied-frontiers [:a 0 :doc]])))
      (is (= {:ok true} (:result (first e2)))))))

(deftest one-in-flight-and-per-stream-order
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        {s2 :state e2 :effects} (step s1 (live :a 1))
        {s3 :state e3 :effects} (step s2 (live :b 0))]
    (is (= 1 (count e1)))
    (is (empty? e2))
    (is (empty? e3))
    (is (= 2 (:queued-items s3)))
    (let [{s4 :state e4 :effects}
          (step s3 {:type :apply-result :token 0 :result {:ok true}})]
      (is (= :a (get-in (second e4) [:publication :origin])))
      (is (= 1 (:token (:in-flight s4))))
      (let [{s5 :state e5 :effects}
            (step s4 {:type :apply-result :token 1 :result {:ok true}})]
        (is (= :b (get-in (second e5) [:publication :origin])))
        (is (= 1 (get-in s5 [:applied-frontiers [:a 0 :doc]])))))))

(deftest out-of-order-one-stream-waits-for-its-prefix
  (let [{s1 :state e1 :effects} (step (state) (live :a 1))
        {s2 :state e2 :effects} (step s1 (live :a 0))]
    (is (empty? e1))
    (is (= 1 (:queued-items s1)))
    (is (= 0 (get-in (first e2) [:publication :seq])))
    (let [{s3 :state e3 :effects}
          (step s2 {:type :apply-result :token 0 :result {:ok true}})]
      (is (= 1 (get-in (second e3) [:publication :seq])))
      (is (= 0 (get-in s3 [:applied-frontiers [:a 0 :doc]]))))))

(deftest snapshot-commit-subsumes-prefix-and-drains-suffix-before-ready
  (let [joining (owner/make-state {:topic :doc :profile "join/1"
                                   :generation generation :phase :joining
                                   :live-transport :overlay})
        s1 (:state (step joining (live :a 0)))
        s2 (:state (step s1 (live :a 1)))
        {s3 :state e3 :effects}
        (step s2 {:type :sync/commit :generation generation
                  :frontiers {[:a 0 :doc] 0}})]
    (is (= [:settle :invoke-live] (mapv :op e3)))
    (is (= 1 (get-in (second e3) [:publication :seq])))
    (is (= :draining (:phase s3)))
    (let [{s4 :state e4 :effects}
          (step s3 {:type :apply-result :token 0 :result {:ok true}})]
      (is (= [:settle :ready] (mapv :op e4)))
      (is (= :live (:phase s4)))
      (is (:ready? s4)))))

(deftest snapshot-capture-waits-for-the-current-application
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        {s2 :state e2 :effects}
        (step s1 {:type :sync/begin :generation generation})
        s3 (:state (step s2 (live :a 1)))
        {s4 :state e4 :effects}
        (step s3 {:type :apply-result :token (:token (first e1))
                  :result {:ok true}})]
    (is (empty? e2) "capture cannot race an in-flight mutation")
    (is (= :joining (:phase s2)))
    (is (= [:settle :begin-sync] (mapv :op e4)))
    (is (= 0 (get-in s4 [:applied-frontiers [:a 0 :doc]])))
    (is (= 1 (:queued-items s4)))
    (let [{s5 :state e5 :effects}
          (step s4 {:type :sync/commit :generation generation
                    :frontiers {[:a 0 :doc] 0}})]
      (is (= :invoke-live (:op (first e5))))
      (is (= 1 (get-in (first e5) [:publication :seq])))
      (is (= :draining (:phase s5))))))

(deftest a-commit-cannot-race-an-in-flight-profile-mutation
  (let [{s1 :state} (step (state) (live :a 0))
        {s2 :state effects :effects}
        (step s1 {:type :sync/commit :generation generation
                  :frontiers {[:a 0 :doc] 0}})]
    (is (= s1 s2))
    (is (= [{:op :sync-protocol-error :topic :doc
             :reason :commit-during-apply}]
           effects))))

(deftest overflow-desynchronizes-once-and-stops-application
  (let [initial (state {:max-queued-items 2 :max-queued-bytes 100})
        {s1 :state} (step initial (live :a 0))
        {s2 :state} (step s1 (live :a 1))
        {s3 :state} (step s2 (live :a 2))
        {s4 :state e4 :effects} (step s3 (live :a 3))
        {s5 :state e5 :effects} (step s4 (live :a 4))]
    (is (= :desynchronized (:phase s4)))
    (is (= [:settle :request-sync] (mapv :op e4)))
    (is (= [:settle] (mapv :op e5)) "recovery is requested exactly once")
    (let [{s6 :state e6 :effects}
          (step s5 {:type :apply-result :token 0 :result {:ok true}})]
      (is (= [:settle] (mapv :op e6)))
      (is (nil? (:in-flight s6)))
      (is (= 2 (:queued-items s6)) "later deltas remain behind repair"))))

(deftest every-nonliteral-result-desynchronizes
  (doseq [result [nil {} {:ok false} {:error :boom}
                  {:ok true :error :contradiction}]]
    (let [{s1 :state e1 :effects} (step (state) (live :a 0))
          {s2 :state e2 :effects}
          (step s1 {:type :apply-result :token (:token (first e1))
                    :result result})]
      (is (= :desynchronized (:phase s2)) (pr-str result))
      (is (= [:settle :request-sync] (mapv :op e2)) (pr-str result)))))

(deftest stale-generation-cannot-mutate-a-replacement
  (let [replacement (owner/make-state {:topic :doc :profile "join/1"
                                       :generation :g2 :phase :live
                                       :live-transport :overlay})
        {s1 :state e1 :effects} (step replacement (live :a 0))
        {s2 :state} (step replacement {:type :sync/commit
                                       :generation generation
                                       :frontiers {[:a 0 :doc] 9}})]
    (is (= :netz/stale-generation (get-in e1 [0 :result :error])))
    (is (empty? (:applied-frontiers s1)))
    (is (empty? (:applied-frontiers s2)))))

(deftest closing-settles-all-receipts-and-ignores-late-results
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        {s2 :state} (step s1 (live :a 1))
        {closed :state effects :effects}
        (step s2 {:type :close :generation generation})
        late (:state (step closed {:type :apply-result
                                   :token (:token (first e1))
                                   :result {:ok true}}))]
    (is (= :closed (:phase closed)))
    (is (= 2 (count (filter #(= :settle (:op %)) effects))))
    (is (empty? (:applied-frontiers late)))))

(deftest many-independent-owners-do-not-share-state
  (let [owners (into {}
                     (for [i (range 1000)]
                       [i (owner/make-state {:topic i :profile "join/1"
                                             :generation generation :phase :live
                                             :live-transport :overlay})]))
        advanced (into {}
                       (for [[i state] owners]
                         [i (:state (step state
                                          (assoc (live i 0)
                                                 :publication
                                                 {:origin i :epoch 0 :topic i
                                                  :seq 0 :payload i :bytes 1})))]))]
    (is (every? #(some? (:in-flight %)) (vals advanced)))
    (is (every? #(zero? (:queued-items %)) (vals advanced)))))
