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

(defn apply-result [invoke result]
  {:type :apply-result :operation-id (:operation-id invoke) :result result})

(defn begin-sync
  ([sync-id] (begin-sync generation sync-id))
  ([generation sync-id]
   {:type :sync/begin :generation generation :sync-id sync-id}))

(defn profile-sync-result [begin result]
  {:type :profile-sync-result
   :operation-id (:operation-id begin)
   :result (cond-> result
             (and (contains? result :frontiers)
                  (not (contains? result :frontier-bytes)))
             (assoc :frontier-bytes (count (:frontiers result))))})

(deftest live-transport-is-exclusive
  (let [{:keys [state effects]}
        (step (state) (assoc (live :a 0) :transport :direct))]
    (is (nil? (:in-flight state)))
    (is (= [:settle :retire-direct] (mapv :op effects)))
    (is (= :netz/wrong-live-transport (get-in effects [0 :result :error])))))

(deftest a-frontier-advances-only-after-exact-apply-success
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        invoke (first e1)]
    (is (= :invoke-live (:op invoke)))
    (is (empty? (:applied-frontiers s1)) "admission is not application")
    (let [{s2 :state e2 :effects}
          (step s1 (apply-result invoke {:ok true}))]
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
          (step s3 (apply-result (first e1) {:ok true}))]
      (is (= :a (get-in (second e4) [:publication :origin])))
      (let [{s5 :state e5 :effects}
            (step s4 (apply-result (second e4) {:ok true}))]
        (is (= :b (get-in (second e5) [:publication :origin])))
        (is (= 1 (get-in s5 [:applied-frontiers [:a 0 :doc]])))))))

(deftest out-of-order-one-stream-waits-for-its-prefix
  (let [{s1 :state e1 :effects} (step (state) (live :a 1))
        {s2 :state e2 :effects} (step s1 (live :a 0))]
    (is (empty? e1))
    (is (= 1 (:queued-items s1)))
    (is (= 0 (get-in (first e2) [:publication :seq])))
    (let [{s3 :state e3 :effects}
          (step s2 (apply-result (first e2) {:ok true}))]
      (is (= 1 (get-in (second e3) [:publication :seq])))
      (is (= 0 (get-in s3 [:applied-frontiers [:a 0 :doc]]))))))

(deftest snapshot-commit-subsumes-prefix-and-drains-suffix-before-ready
  (let [joining (owner/make-state {:topic :doc :profile "join/1"
                                   :generation generation :phase :joining
                                   :live-transport :overlay})
        s1 (:state (step joining (live :a 0)))
        s2 (:state (step s1 (live :a 1)))
        {s3 :state begin-effects :effects} (step s2 (begin-sync :sync-1))
        {s4 :state commit-effects :effects}
        (step s3 (profile-sync-result
                  (first begin-effects)
                  {:ok true :frontiers {[:a 0 :doc] 0}}))]
    (is (= [:settle :invoke-live] (mapv :op commit-effects)))
    (is (= 1 (get-in (second commit-effects) [:publication :seq])))
    (is (= :draining (:phase s4)))
    (let [{s5 :state e5 :effects}
          (step s4 (apply-result (second commit-effects) {:ok true}))]
      (is (= [:settle :ready] (mapv :op e5)))
      (is (= :live (:phase s5)))
      (is (:ready? s5)))))

(deftest snapshot-capture-waits-for-current-application
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        {s2 :state e2 :effects} (step s1 (begin-sync :sync-1))
        s3 (:state (step s2 (live :a 1)))
        {s4 :state e4 :effects}
        (step s3 (apply-result (first e1) {:ok true}))]
    (is (empty? e2) "capture cannot race an in-flight mutation")
    (is (= :joining (:phase s2)))
    (is (= [:settle :begin-sync] (mapv :op e4)))
    (is (= 0 (get-in s4 [:applied-frontiers [:a 0 :doc]])))
    (is (= 1 (:queued-items s4)))
    (let [{s5 :state e5 :effects}
          (step s4 (profile-sync-result
                    (second e4)
                    {:ok true :frontiers {[:a 0 :doc] 0}}))]
      (is (= :invoke-live (:op (first e5))))
      (is (= 1 (get-in (first e5) [:publication :seq])))
      (is (= :draining (:phase s5))))))

(deftest profile-commit-cannot-race-in-flight-mutation
  (let [{s1 :state} (step (state) (live :a 0))
        {s2 :state} (step s1 (begin-sync :sync-1))
        operation-id (:active-sync s2)
        {s3 :state effects :effects}
        (step s2 {:type :profile-sync-result :operation-id operation-id
                  :result {:ok true :frontiers {[:a 0 :doc] 0}}})]
    (is (= s2 s3))
    (is (= [{:op :sync-protocol-error :topic :doc
             :reason :commit-during-apply}]
           effects))))

(deftest failed-overlay-apply-does-not-lose-pending-snapshot
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        s2 (:state (step s1 (begin-sync :sync-1)))
        {s3 :state effects :effects}
        (step s2 (apply-result (first e1) {:error :boom}))]
    (is (= :joining (:phase s3)))
    (is (false? (:sync-begin-pending? s3)))
    (is (= [:settle :begin-sync] (mapv :op effects)))))

(deftest failed-direct-apply-retires-before-a-replacement-sync
  (let [direct (owner/make-state {:topic :doc :profile "join/1"
                                  :generation generation :phase :live
                                  :live-transport :direct})
        {s1 :state e1 :effects}
        (step direct (assoc (live :a 0) :transport :direct))
        s2 (:state (step s1 (begin-sync :sync-1)))
        {failed :state effects :effects}
        (step s2 (apply-result (first e1) {:error :boom}))]
    (is (= :failed (:phase failed)))
    (is (= [:settle :retire-direct] (mapv :op effects)))
    (is (not-any? #(= :begin-sync (:op %)) effects))))

(deftest ordinary-direct-failures-retire-without-requesting-sync
  (let [direct (owner/make-state {:topic :doc :profile "join/1"
                                  :generation generation :phase :live
                                  :live-transport :direct})
        {s1 :state invoke :effects}
        (step direct (assoc (live :a 0) :transport :direct))
        {failed :state effects :effects}
        (step s1 (apply-result (first invoke) {:ok false}))]
    (is (= :failed (:phase failed)))
    (is (= [:settle :retire-direct] (mapv :op effects)))
    (is (not-any? #(= :request-sync (:op %)) effects))))

(deftest overflow-desynchronizes-once-and-stops-application
  (let [initial (state {:max-queued-items 2 :max-queued-bytes 100})
        {s1 :state e1 :effects} (step initial (live :a 0))
        {s2 :state} (step s1 (live :a 1))
        {s3 :state} (step s2 (live :a 2))
        {s4 :state e4 :effects} (step s3 (live :a 3))
        {s5 :state e5 :effects} (step s4 (live :a 4))]
    (is (= :desynchronized (:phase s4)))
    (is (= [:settle :request-sync] (mapv :op e4)))
    (is (= [:settle] (mapv :op e5)) "recovery is requested exactly once")
    (let [{s6 :state e6 :effects}
          (step s5 (apply-result (first e1) {:ok true}))]
      (is (= [:settle] (mapv :op e6)))
      (is (nil? (:in-flight s6)))
      (is (= 2 (:queued-items s6)) "later deltas remain behind repair"))))

(deftest overflow-during-sync-fails-and-settles-the-generation
  (let [initial (state {:max-queued-items 1})
        {joining :state} (step initial (begin-sync :sync-1))
        {queued :state} (step joining (live :a 0))
        {failed :state effects :effects} (step queued (live :a 1))]
    (is (= :failed (:phase failed)))
    (is (= 2 (count (filter #(= :settle (:op %)) effects))))
    (is (empty? (:queue failed)))
    (is (nil? (:active-sync failed)))))

(deftest every-nonliteral-result-desynchronizes
  (doseq [result [nil false {} {:ok false} {:error :boom}
                  {:ok true :error nil} {:ok true :error false}]]
    (let [{s1 :state e1 :effects} (step (state) (live :a 0))
          {s2 :state e2 :effects}
          (step s1 (apply-result (first e1) result))]
      (is (= :desynchronized (:phase s2)) (pr-str result))
      (is (= [:settle :request-sync] (mapv :op e2)) (pr-str result)))))

(deftest late-apply-result-cannot-cross-owner-generations
  (let [{old-state :state old-effects :effects} (step (state) (live :a 0))
        replacement (owner/make-state {:topic :doc :profile "join/1"
                                       :generation :g2 :phase :live
                                       :live-transport :overlay})
        {new-state :state new-effects :effects}
        (step replacement (assoc (live :a 0) :generation :g2))
        old-result (apply-result (first old-effects) {:ok true})
        after-late (:state (step new-state old-result))]
    (is (= 0 (get-in old-state [:in-flight :operation-id :token])))
    (is (= 0 (get-in new-state [:in-flight :operation-id :token])))
    (is (not= (:operation-id (first old-effects))
              (:operation-id (first new-effects))))
    (is (= new-state after-late))
    (is (empty? (:applied-frontiers after-late)))))

(deftest owner-construction-enforces-routing-and-capacity-invariants
  (doseq [overrides [{:topic nil}
                     {:profile nil}
                     {:generation nil}
                     {:live-transport :both}
                     {:phase :unknown}
                     {:opts {:max-queued-items -1}}
                     {:applied-frontiers {[:a -1 :doc] 0}}]]
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                 (owner/make-state
                  (merge {:topic :doc :profile "join/1"
                          :generation generation :phase :live}
                         overrides)))
        (pr-str overrides))))

(deftest sync-attempts-are-correlated-and-cannot-be-reordered
  (let [{s1 :state e1 :effects} (step (state) (begin-sync :sync-1))
        {s2 :state e2 :effects} (step s1 (begin-sync :sync-2))
        stale {:type :profile-sync-result
               :operation-id {:generation generation :sync-id :sync-2}
               :result {:ok true :frontiers {[:a 0 :doc] 99}}}
        after-stale (:state (step s2 stale))
        committed (:state
                   (step after-stale
                         (profile-sync-result
                          (first e1)
                          {:ok true :frontiers {[:a 0 :doc] 0}})))]
    (is (= :sync-already-active (:reason (first e2))))
    (is (= s2 after-stale))
    (is (= 0 (get-in committed [:applied-frontiers [:a 0 :doc]])))))

(deftest reused-sync-id-gets-a-new-owner-minted-attempt
  (let [{s1 :state first-effects :effects} (step (state) (begin-sync :same-id))
        first-begin (first first-effects)
        after-first (:state
                     (step s1 (profile-sync-result
                               first-begin
                               {:ok true :frontiers {[:a 0 :doc] 0}})))
        {s2 :state second-effects :effects}
        (step after-first (begin-sync :same-id))
        second-begin (first second-effects)
        after-late (:state
                    (step s2 (profile-sync-result
                              first-begin
                              {:ok true :frontiers {[:a 0 :doc] 999}})))]
    (is (not= (:operation-id first-begin) (:operation-id second-begin)))
    (is (= s2 after-late))
    (is (= 0 (get-in after-late [:applied-frontiers [:a 0 :doc]])))))

(deftest nil-operation-cannot-commit-or-abort-an-unstarted-sync
  (let [joining (owner/make-state {:topic :doc :profile "join/1"
                                   :generation generation :phase :joining})
        committed (:state
                   (step joining {:type :profile-sync-result
                                  :operation-id nil
                                  :result {:ok true :frontiers {[:a 0 :doc] 99}
                                           :frontier-bytes 1}}))
        aborted (:state (step joining {:type :sync/aborted
                                       :operation-id nil}))]
    (is (= joining committed))
    (is (= joining aborted))))

(deftest raw-or-unbound-frontier-cannot-commit
  (let [raw (:state (step (state) {:type :sync/commit
                                   :generation generation
                                   :frontiers {[:a 0 :doc] 1000000}}))
        unbound (:state (step (state) {:type :profile-sync-result
                                       :operation-id {:generation generation
                                                      :sync-id :forged}
                                       :result {:ok true
                                                :frontiers {[:a 0 :doc] 1000000}}}))]
    (is (empty? (:applied-frontiers raw)))
    (is (empty? (:applied-frontiers unbound)))))

(deftest foreign-topic-frontiers-and-publications-are-rejected
  (let [{s1 :state e1 :effects}
        (step (state) (assoc-in (live :a 0) [:publication :topic] :other))
        {s2 :state begin-effects :effects} (step (state) (begin-sync :sync-1))
        {failed :state result-effects :effects}
        (step s2 (profile-sync-result
                  (first begin-effects)
                  {:ok true :frontiers {[:a 0 :other] 99}}))]
    (is (= s1 (state)))
    (is (= :netz/invalid-publication (get-in e1 [0 :result :error])))
    (is (= :failed (:phase failed)))
    (is (empty? (:applied-frontiers failed)))
    (is (empty? result-effects))))

(deftest frontier-metadata-is-bounded-and-mandatory
  (let [{joining :state begin-effects :effects}
        (step (state {:max-frontier-streams 1 :max-frontier-bytes 4})
              (begin-sync :sync-1))
        begin (first begin-effects)]
    (doseq [result [{:ok true :frontiers {[:a 0 :doc] 0}}
                    {:ok true :frontiers {[:a 0 :doc] 0}
                     :frontier-bytes 5}
                    {:ok true :frontiers {[:a 0 :doc] 0 [:b 0 :doc] 0}
                     :frontier-bytes 2}
                    {:ok true :frontiers {[nil 0 :doc] 0}
                     :frontier-bytes 1}]]
      (let [{failed :state}
            (step joining {:type :profile-sync-result
                           :operation-id (:operation-id begin)
                           :result result})]
        (is (= :failed (:phase failed)) (pr-str result))))))

(deftest sequence-and-epoch-use-the-portable-exact-integer-range
  (let [maximum owner/max-safe-integer
        {accepted :state} (step (state) (live :a maximum))]
    (is (= 1 (:queued-items accepted)) "a safe but gapped sequence is retained")
    (doseq [publication [(assoc (:publication (live :a 0))
                                :seq (inc maximum))
                         (assoc (:publication (live :a 0))
                                :epoch (inc maximum))]]
      (let [{rejected :state effects :effects}
            (step (state) (assoc (live :a 0) :publication publication))]
        (is (nil? (:in-flight rejected)))
        (is (= :netz/invalid-publication
               (get-in effects [0 :result :error])))))))

(deftest malformed-publication-accounting-is-rejected-without-throwing
  (doseq [bytes [nil -1 "ten" 1.5]]
    (let [{:keys [state effects]}
          (step (state) (assoc-in (live :a 0) [:publication :bytes] bytes))]
      (is (nil? (:in-flight state)) (pr-str bytes))
      (is (= :netz/invalid-publication
             (get-in effects [0 :result :error]))
          (pr-str bytes)))))

(deftest sync-abort-settles-waiters-and-never-becomes-ready
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        {s2 :state} (step s1 (live :a 1))
        {s3 :state} (step s2 (begin-sync :sync-1))
        {failed :state effects :effects}
        (step s3 {:type :sync/aborted :operation-id (:active-sync s3)})]
    (is (= :failed (:phase failed)))
    (is (false? (:ready? failed)))
    (is (= 2 (count (filter #(= :settle (:op %)) effects))))
    (is (= failed (:state (step failed (apply-result (first e1) {:ok true})))))))

(deftest closed-and-failed-owners-are-absorbing
  (let [closed (:state (step (state) {:type :close :generation generation}))
        after-close (:state (step closed (begin-sync :sync-1)))
        {joining :state begin-effects :effects} (step (state) (begin-sync :sync-2))
        failed (:state
                (step joining
                      (profile-sync-result (first begin-effects) {:error :boom})))
        after-failure (:state (step failed (begin-sync :sync-3)))]
    (is (= closed after-close))
    (is (= :closed (:phase after-close)))
    (is (= failed after-failure))
    (is (= :failed (:phase after-failure)))))

(deftest duplicate-waiters-are-bounded-independently-of-queue-capacity
  (let [{s1 :state} (step (state {:max-queued-items 1
                                  :max-duplicate-waiters 1})
                          (live :a 0))
        {:keys [state rejected]}
        (reduce (fn [{:keys [state rejected]} i]
                  (let [{next-state :state effects :effects}
                        (step state (live :a 0 {:receipt [:duplicate i]}))]
                    {:state next-state
                     :rejected (+ rejected (count effects))}))
                {:state s1 :rejected 0}
                (range 10000))]
    (is (= 1 (count (get-in state [:in-flight :receipts]))))
    (is (= 10000 rejected))
    (is (zero? (:queued-items state)))))

(deftest closing-settles-all-receipts-and-ignores-late-results
  (let [{s1 :state e1 :effects} (step (state) (live :a 0))
        {s2 :state} (step s1 (live :a 1))
        {closed :state effects :effects}
        (step s2 {:type :close :generation generation})
        late (:state (step closed (apply-result (first e1) {:ok true})))]
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
