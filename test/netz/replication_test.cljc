(ns netz.replication-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication :as replication]
            [netz.replication.placement :as placement]))

(defn authority [pair] (apply str (repeat 32 pair)))

(def members
  [{:id (authority "aa") :domains {"party" "one" "region" "north"}}
   {:id (authority "bb") :domains {"party" "two" "region" "south"}}
   {:id (authority "cc") :domains {"party" "three" "region" "west"}}])

(def layout {:epoch 7 :members members})
(def policy {:replicas 3 :write-acks 2 :minimum-domains {"party" 2}})

(defn step [state event]
  (replication/handler state event {:now 0}))

(defn begin-ready [state manifest]
  (let [{intent-state :state intent-actions :actions}
        (step state {:type :replication/begin :manifest manifest})
        object-id (:object-id manifest)
        epoch (get-in intent-state [:layout :epoch])
        ready (step intent-state {:type :replication/intent-stored
                                  :object-id object-id
                                  :layout-epoch epoch})]
    (assoc ready :intent-actions intent-actions)))

(deftest rendezvous-placement-is-deterministic
  (let [a (placement/place layout policy "object-1")
        b (placement/place (update layout :members #(vec (reverse %)))
                           policy "object-1")]
    (is (= a b) "layout vector order must carry no placement meaning")
    (is (= (set (map :id members)) (set (map :id a))))
    (is (= 32 (identity/buf-length
               (placement/score 7 "object-1" (authority "aa")))))))

(deftest placement-known-answer-matches-the-python-fixture
  (let [ids (mapv #(apply str (repeat 32 %)) ["00" "11" "22" "33"])
        owners ["alice" "alice" "bob" "carol"]
        sites ["alice-home" "alice-cloud" "bob-home" "carol-home"]
        fixture-layout
        {:epoch 7
         :members (mapv (fn [id owner site]
                          {:id id :domains {"owner" owner "site" site}})
                        ids owners sites)}
        object-id (identity/hex->bytes (apply str (repeat 32 "a0")))
        expected-scores
        ["a13b7f83df6af53933e0d3d11f08b1610dd4dbaedeff819ea8f2af0282cf32d0"
         "69a677f5ae75c0e3934a2303d34733116f1144334cc39af6df2a056867ee4d00"
         "d360ebeabef0dedd1ea60a7b9a08f9a9aaa268d0f77d9245e01064f982abc88a"
         "e5585181c3d32716ade7812b374e0576de4774972f3dcd98ccadc17e0c3bc606"]
        expected-ranked [(nth ids 3) (nth ids 2) (nth ids 0) (nth ids 1)]]
    (is (= expected-scores
           (mapv #(identity/bytes->hex (placement/score 7 object-id %)) ids)))
    (is (= expected-ranked
           (mapv :id (placement/rank fixture-layout object-id))))
    (is (= (subvec expected-ranked 0 3)
           (mapv :id
                 (placement/place
                  fixture-layout
                  {:replicas 3 :write-acks 2
                   :minimum-domains {"owner" 2 "site" 2}}
                  object-id))))))

(deftest disabled-members-are-not-placed
  (let [four {:epoch 0
              :members (conj members
                             {:id (authority "dd") :enabled? false
                              :domains {"party" "four"}})}]
    (is (not (contains? (set (map :id (placement/place four policy "x")))
                        (authority "dd"))))))

(deftest constrained-placement-reaches-beyond-the-preferred-set
  ;; Discover the unconstrained ranking, then deliberately put its first two
  ;; members in one party. A two-replica placement must skip one of them.
  (let [base {:epoch 3
              :members (mapv (fn [id] {:id id :domains {}})
                             (map authority ["aa" "bb" "cc" "dd"]))}
        ranked (placement/rank base "constrained")
        [first-id second-id] (map :id ranked)
        adjusted (update base :members
                         (fn [ms]
                           (mapv (fn [m]
                                   (assoc m :domains
                                          {"party" (if (#{first-id second-id} (:id m))
                                                     "same" "other")}))
                                 ms)))
        selected (placement/place adjusted
                                  {:replicas 2 :write-acks 2
                                   :minimum-domains {"party" 2}}
                                  "constrained")]
    (is (= 2 (get (placement/domain-counts selected {"party" 2}) "party")))
    (is (not= #{first-id second-id} (set (map :id selected))))))

(deftest invalid-and-impossible-policies-are-refused
  (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                        #"Invalid replica policy"
                        (placement/place layout
                                         {:replicas 3 :write-acks 4}
                                         "x")))
  (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                        #"Invalid replica policy"
                        (placement/place layout
                                         {:replicas 3 :write-acks 1
                                          :minimum-domains {"party" 2}}
                                         "x")))
  (is (thrown-with-msg? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                        #"cannot satisfy"
                        (placement/place
                         {:epoch 0
                          :members [{:id (authority "aa")
                                     :domains {"party" "one"}}
                                    {:id (authority "bb")
                                     :domains {"party" "one"}}]}
                         {:replicas 2 :write-acks 2
                          :minimum-domains {"party" 2}}
                         "x"))))

(deftest receipts-follow-durable-storage-events
  (let [initial (replication/make-state layout policy)
        manifest {:object-id "blob" :length 10}
        {begun :state stores :actions intent-actions :intent-actions}
        (begin-ready initial manifest)
        targets (mapv second stores)
        epoch (:epoch layout)]
    (is (= 3 (count stores)))
    (is (= :persist-intent (ffirst intent-actions)))
    (is (every? #(= :store (first %)) stores))

    (testing "one durable copy is not a quorum"
      (let [{one :state actions :actions}
            (step begun {:type :replication/stored
                         :object-id "blob" :replica-id (first targets)
                         :layout-epoch epoch :attempt 0})]
        (is (empty? actions))

        (testing "a duplicate cannot manufacture another vote"
          (let [{dup :state actions :actions}
                (step one {:type :replication/stored
                           :object-id "blob" :replica-id (first targets)
                           :layout-epoch epoch :attempt 0})]
            (is (empty? actions))
            (is (= 1 (get-in dup [:stats :duplicates])))))

        (testing "two eligible parties produce exactly one quorum receipt"
          (let [{two :state actions :actions}
                (step one {:type :replication/stored
                           :object-id "blob" :replica-id (second targets)
                           :layout-epoch epoch :attempt 0})]
            (is (= [:durable] (mapv first actions)))
            (is (= :quorum (get-in actions [0 1 :status])))
            (is (= 2 (count (get-in actions [0 1 :durable-acks]))))

            (testing "the final target produces completion without re-emitting quorum"
              (let [{done :state actions :actions}
                    (step two {:type :replication/stored
                               :object-id "blob" :replica-id (nth targets 2)
                               :layout-epoch epoch :attempt 0})]
                (is (= [:complete] (mapv first actions)))
                (is (= 3 (count (get-in actions [0 1 :durable-acks]))))
                (is (= 1 (get-in done [:stats :durable])))
                (is (= 1 (get-in done [:stats :complete])))))))))))

(deftest acknowledgements-must-name-a-current-target-and-layout
  (let [{state :state}
        (begin-ready (replication/make-state layout policy)
                     {:object-id "blob"})
        events [{:type :replication/stored :object-id "blob"
                 :replica-id (authority "dd") :layout-epoch 7}
                {:type :replication/stored :object-id "blob"
                 :replica-id (authority "aa") :layout-epoch 6 :attempt 0}
                {:type :replication/stored :object-id "missing"
                 :replica-id (authority "aa") :layout-epoch 7 :attempt 0}]
        final (reduce (fn [s e] (:state (step s e))) state events)]
    (is (= 3 (get-in final [:stats :invalid-acks])))
    (is (empty? (get-in final [:objects "blob" :acks])))))

(deftest one-address-cannot-name-two-manifests
  (let [{state :state}
        (begin-ready (replication/make-state layout policy)
                     {:object-id "same" :length 1})]
    (is (thrown-with-msg?
         #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
         #"conflicting manifests"
         (step state {:type :replication/begin
                      :manifest {:object-id "same" :length 2}})))))

(deftest repair-invalidates-stale-acknowledgements
  (let [initial (replication/make-state layout policy)
        {begun :state stores :actions}
        (begin-ready initial {:object-id "blob"})
        target (second (first stores))
        epoch (:epoch layout)
        {lost :state retry :actions}
        (step begun {:type :replication/lost :object-id "blob"
                     :replica-id target :layout-epoch epoch})]
    (is (= 1 (get-in retry [0 3 :attempt])))
    (testing "a delayed completion from the old disk generation is ignored"
      (let [{stale :state actions :actions}
            (step lost {:type :replication/stored :object-id "blob"
                        :replica-id target :layout-epoch epoch :attempt 0})]
        (is (empty? actions))
        (is (empty? (get-in stale [:objects "blob" :acks])))))
    (testing "the retried durable installation is accepted"
      (let [{current :state}
            (step lost {:type :replication/stored :object-id "blob"
                        :replica-id target :layout-epoch epoch :attempt 1})]
        (is (= #{target} (get-in current [:objects "blob" :acks])))))))

(deftest stores-cannot-leave-before-the-intent-is-durable
  (let [manifest {:object-id "blob"}
        {state :state actions :actions}
        (step (replication/make-state layout policy)
              {:type :replication/begin :manifest manifest})
        target (first (get-in state [:objects "blob" :targets]))
        premature (step state {:type :replication/stored
                               :object-id "blob" :replica-id target
                               :layout-epoch 7 :attempt 0})]
    (is (= [:persist-intent] (mapv first actions)))
    (is (empty? (:actions premature)))
    (is (empty? (get-in (:state premature) [:objects "blob" :acks])))))
