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

(def layout {:placement "netz/blob-placement/2" :seed (apply str (repeat 64 "a")) :epoch 7 :members members})
(def policy {:replicas 3 :write-acks 2 :minimum-domains {"party" 2}})

(deftest main-placement-is-stable-and-requires-explicit-configuration
  (doseq [object (map #(str "object-" %) (range 200))]
    (is (= (placement/place layout policy object)
           (placement/place (assoc layout :epoch 999) policy object))))
  (doseq [invalid [(dissoc layout :seed)
                   (dissoc layout :placement)
                   (assoc layout :placement "netz/blob-placement/1")
                   (assoc layout :seed "bad")]]
    (is (thrown? #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
                 (placement/rank invalid "object")))))

(defn step [state event]
  (replication/handler state event {:now 0}))

(defn begin-ready [state manifest]
  (let [{intent-state :state intent-actions :actions}
        (step state {:type :replication/begin :manifest manifest})
        object-id (:object-id manifest)
        epoch (get-in intent-state [:layout :epoch])
        ready (step intent-state {:type :replication/intent-stored
                                  :object-id object-id
                                  :layout-epoch epoch
                                  :intent-revision 0})]
    (assoc ready :intent-actions intent-actions)))

(deftest rendezvous-placement-is-deterministic
  (let [a (placement/place layout policy "object-1")
        b (placement/place (update layout :members #(vec (reverse %)))
                           policy "object-1")]
    (is (= a b) "layout vector order must carry no placement meaning")
    (is (= (set (map :id members)) (set (map :id a))))
    (is (= 32 (identity/buf-length
               (placement/score (:seed layout) "object-1" (authority "aa")))))))

(deftest placement-known-answer-matches-the-python-fixture
  (let [ids (mapv #(apply str (repeat 32 %)) ["00" "11" "22" "33"])
        owners ["alice" "alice" "bob" "carol"]
        sites ["alice-home" "alice-cloud" "bob-home" "carol-home"]
        fixture-layout
        {:placement "netz/blob-placement/2" :seed (apply str (repeat 64 "a")) :epoch 7
         :members (mapv (fn [id owner site]
                          {:id id :domains {"owner" owner "site" site}})
                        ids owners sites)}
        object-id (identity/hex->bytes (apply str (repeat 32 "a0")))
        expected-scores
        ["dc3ca415c7be188e5a753c28918f40029bc93259eca7153639f83d87e9441992"
         "8cc53c03b29ed39d3476200d9d7bf1d94f180ed2ff4df892d0fe78844adec7bf"
         "9bf58a033d94e93b8adbbcbb771a038dd72494f4479486d5d878339f554dc729"
         "cfb18bba704df8c9388aafd7617da49d453bffb7da37d61a53b243623de79889"]
        expected-ranked [(nth ids 0) (nth ids 3) (nth ids 2) (nth ids 1)]]
    (is (= expected-scores
           (mapv #(identity/bytes->hex (placement/score (:seed fixture-layout) object-id %)) ids)))
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
  (let [four {:placement "netz/blob-placement/2" :seed (apply str (repeat 64 "a")) :epoch 0
              :members (conj members
                             {:id (authority "dd") :enabled? false
                              :domains {"party" "four"}})}]
    (is (not (contains? (set (map :id (placement/place four policy "x")))
                        (authority "dd"))))))

(deftest constrained-placement-reaches-beyond-the-preferred-set
  ;; Discover the unconstrained ranking, then deliberately put its first two
  ;; members in one party. A two-replica placement must skip one of them.
  (let [base {:placement "netz/blob-placement/2" :seed (apply str (repeat 64 "a")) :epoch 3
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
                         {:placement "netz/blob-placement/2" :seed (apply str (repeat 64 "a")) :epoch 0
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
    (is (= [:persist-intent] (mapv first retry)))
    (is (= 1 (get-in lost [:objects "blob" :attempts target])))
    (is (false? (get-in lost [:objects "blob" :intent-stored?])))
    (testing "neither a stale receipt nor stale outbox completion escapes"
      (let [{stale :state actions :actions}
            (step lost {:type :replication/stored :object-id "blob"
                        :replica-id target :layout-epoch epoch :attempt 0})
            stale-persist
            (step stale {:type :replication/intent-stored
                         :object-id "blob" :layout-epoch epoch
                         :intent-revision 0})]
        (is (empty? actions))
        (is (empty? (:actions stale-persist)))
        (is (empty? (get-in (:state stale-persist)
                            [:objects "blob" :acks])))))
    (testing "the retried durable installation is accepted"
      (let [{ready :state stores :actions}
            (step lost {:type :replication/intent-stored
                        :object-id "blob" :layout-epoch epoch
                        :intent-revision 1})
            {current :state}
            (step ready {:type :replication/stored :object-id "blob"
                         :replica-id target :layout-epoch epoch :attempt 1})]
        (is (= [:store] (mapv first stores)))
        (is (= 1 (get-in stores [0 3 :attempt])))
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

(deftest signed-intent-revision-invalidates-receipts-before-reoffer
  (let [{installed :state initial-stores :actions}
        (begin-ready (replication/make-state layout policy)
                     {:object-id "blob" :manifest-id "manifest-v1"})
        target (second (first initial-stores))
        {acknowledged :state}
        (step installed {:type :replication/stored
                         :object-id "blob" :replica-id target
                         :layout-epoch 7 :attempt 0})
        next-attempts (into {}
                            (map (fn [[replica attempt]]
                                   [replica (inc attempt)]))
                            (get-in acknowledged [:objects "blob" :attempts]))
        successor {:manifest {:object-id "blob"
                              :manifest-id "manifest-v1"}
                   :layout layout :policy policy
                   :attempts next-attempts :intent-revision 1}
        {pending :state actions :actions}
        (step acknowledged {:type :replication/revise :intent successor})]
    (is (= [:persist-intent] (mapv first actions)))
    (is (empty? (get-in pending [:objects "blob" :acks])))
    (is (false? (get-in pending [:objects "blob" :intent-stored?])))
    (let [{ready :state stores :actions}
          (step pending {:type :replication/intent-stored
                         :object-id "blob" :layout-epoch 7
                         :intent-revision 1})
          stale (step ready {:type :replication/stored
                             :object-id "blob" :replica-id target
                             :layout-epoch 7 :attempt 0})]
      (is (= 3 (count stores)))
      (is (every? #(= 1 (get-in % [3 :attempt])) stores))
      (is (empty? (get-in (:state stale) [:objects "blob" :acks]))))))

(deftest placement-policy-is-bound-per-intent
  (let [two-copy-policy {:replicas 2 :write-acks 1
                         :minimum-domains {"party" 1}}
        {begun :state}
        (step (replication/make-state layout policy)
              {:type :replication/begin
               :manifest {:object-id "small"}
               :layout layout
               :policy two-copy-policy})
        {ready :state stores :actions}
        (step begun {:type :replication/intent-stored
                     :object-id "small" :layout-epoch 7
                     :intent-revision 0})
        target (second (first stores))
        {durable :state emitted :actions}
        (step ready {:type :replication/stored
                     :object-id "small" :replica-id target
                     :layout-epoch 7 :attempt 0})]
    (is (= 2 (count stores)))
    (is (= two-copy-policy (get-in ready [:objects "small" :policy])))
    (is (= [:durable] (mapv first emitted)))
    (is (true? (get-in durable [:objects "small" :durable-emitted?])))))

(deftest verified-intents-restore-without-releasing-network-work
  (let [targets (mapv :id (placement/place layout policy "restored"))
        attempts (zipmap targets [3 4 5])
        restored
        (replication/restore-state
         (replication/make-state layout policy)
         [{:manifest {:object-id "restored"}
           :layout layout :policy policy
           :attempts attempts :intent-revision 9}])
        {pending :state actions :actions}
        (step restored {:type :replication/repair :object-id "restored"})]
    (is (= attempts (get-in restored [:objects "restored" :attempts])))
    (is (empty? (get-in restored [:objects "restored" :pending-targets])))
    (is (= [:persist-intent] (mapv first actions)))
    (is (= 10 (get-in pending [:objects "restored" :intent-revision])))
    (is (= (zipmap targets [4 5 6])
           (get-in pending [:objects "restored" :attempts])))
    (is (thrown-with-msg?
         #?(:clj clojure.lang.ExceptionInfo :cljs js/Error)
         #"Invalid durable replication intent"
         (replication/restore-state
          (replication/make-state layout policy)
          [{:manifest {:object-id "bad"}
            :layout layout :policy policy
            :attempts {(first targets) 0} :intent-revision 0}])))))
