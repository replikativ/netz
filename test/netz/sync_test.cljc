(ns netz.sync-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.overlay :as overlay]
            [netz.sim :as sim]
            [netz.sync :as sync]
            [netz.sync.join-semilattice :as join]
            [netz.sync.profile :as profile]
            [netz.sync.root-log :as root-log]))

(def root-profile (root-log/profile :writer))
(def root-profile-id (profile/profile-id root-profile))
(def join-profile (join/profile 1))
(def join-profile-id (profile/profile-id join-profile))

(defn sync-node [id profile-id impl topic value & [opts]]
  (sync/make-state id
                   (merge {:profiles {profile-id impl}
                           :applications {topic value}}
                          opts)))

(defn pull [s requester responder topic profile-id session]
  (-> s
      (sim/send-message :app requester
                        {:type :sync/start :peer responder :topic topic
                         :profile profile-id :session session})
      (sim/run-until (+ (:now s) 1000))))

(deftest reference-join-obeys-semilattice-laws
  (let [join-value (fn [a b]
                     (profile/commit join-profile a :doc b b))
        a #{1 2} b #{2 3} c #{3 4}]
    (is (= (join-value a b) (join-value b a)) "commutative")
    (is (= (join-value (join-value a b) c)
           (join-value a (join-value b c)))
        "associative")
    (is (= a (join-value a a)) "idempotent")))

(deftest fenced-root-session-commits-atomically
  (let [topic "db"
        origin (apply str (repeat 64 "0"))
        old {:writer :writer :term 1 :index 4 :root :old}
        current {:writer :writer :term 2 :index 7 :root :current}
        s (-> (sim/make-sim {:seed 8 :latency-min 1 :latency-max 10})
              (sim/add-node :reader sync/handler
                            (sync-node :reader root-profile-id root-profile topic old))
              (sim/add-node :writer sync/handler
                            (sync-node :writer root-profile-id root-profile topic current
                                       {:frontiers {topic {[origin 0 topic] 7}}}))
              (pull :reader :writer topic root-profile-id :root-session))
        reader (sim/node-state s :reader)]
    (is (= current (sync/application reader topic)))
    (is (= :synchronized (sync/status reader topic)))
    (is (= 1 (get-in s [:stats :sync-committed])))
    (is (empty? (:sessions reader))))

  (testing "a conflicting root at the same fence/index is rejected"
    (let [a {:writer :writer :term 3 :index 9 :root :a}
          b {:writer :writer :term 3 :index 9 :root :b}]
      (is (thrown? #?(:clj Exception :cljs js/Error)
                   (profile/apply-chunk root-profile a :db b))))))

(deftest join-semilattice-converges-after-pairwise-sync
  (let [base (-> (sim/make-sim {:seed 12 :latency-min 1 :latency-max 8})
                 (sim/add-node :a sync/handler
                               (sync-node :a join-profile-id join-profile :doc #{:a}))
                 (sim/add-node :b sync/handler
                               (sync-node :b join-profile-id join-profile :doc #{:b}))
                 (sim/add-node :c sync/handler
                               (sync-node :c join-profile-id join-profile :doc #{:c})))
        converged (-> base
                      (pull :a :b :doc join-profile-id :ab)
                      (pull :a :c :doc join-profile-id :ac)
                      (pull :b :a :doc join-profile-id :ba)
                      (pull :c :a :doc join-profile-id :ca))]
    (doseq [id [:a :b :c]]
      (is (= #{:a :b :c}
             (sync/application (sim/node-state converged id) :doc))
          (str id " did not converge")))))

(deftest join-semilattice-recovers-after-partition-and-timeout
  (let [partitioned (-> (sim/make-sim {:seed 7 :latency-min 1 :latency-max 5})
                        (sim/add-node :a sync/handler
                                      (sync-node :a join-profile-id join-profile
                                                 :doc #{:a}))
                        (sim/add-node :b sync/handler
                                      (sync-node :b join-profile-id join-profile
                                                 :doc #{:b}))
                        (sim/partition-network {:a :left :b :right})
                        (sim/send-message :app :a
                                          {:type :sync/start :peer :b :topic :doc
                                           :profile join-profile-id
                                           :session :partitioned})
                        (sim/run-until 20000))]
    (is (= :desynchronized (sync/status (sim/node-state partitioned :a) :doc)))
    (let [healed (-> partitioned
                     (sim/heal)
                     (pull :a :b :doc join-profile-id :after-heal-a)
                     (pull :b :a :doc join-profile-id :after-heal-b))]
      (is (= #{:a :b} (sync/application (sim/node-state healed :a) :doc)))
      (is (= #{:a :b} (sync/application (sim/node-state healed :b) :doc))))))

(deftest three-peer-profiles-converge-after-partition-and-rejoin
  (testing "multiwriter state joins contributions made on both sides"
    (let [partitioned
          (-> (sim/make-sim {:seed 71 :latency-min 1 :latency-max 8})
              (sim/add-node :a sync/handler
                            (sync-node :a join-profile-id join-profile :doc #{:a}))
              (sim/add-node :b sync/handler
                            (sync-node :b join-profile-id join-profile :doc #{:b}))
              (sim/add-node :c sync/handler
                            (sync-node :c join-profile-id join-profile :doc #{:c}))
              (sim/partition-network {:a :left :b :left :c :right})
              (pull :a :b :doc join-profile-id :left-ab)
              (pull :b :a :doc join-profile-id :left-ba)
              (sim/send-message :app :c
                                {:type :sync/start :peer :a :topic :doc
                                 :profile join-profile-id :session :cut})
              (sim/run-until 25000))
          healed (-> partitioned
                     (sim/heal)
                     (pull :a :c :doc join-profile-id :heal-ac)
                     (pull :c :a :doc join-profile-id :heal-ca)
                     (pull :b :a :doc join-profile-id :heal-ba))]
      (is (= :desynchronized (sync/status (sim/node-state partitioned :c) :doc)))
      (doseq [peer [:a :b :c]]
        (is (= #{:a :b :c}
               (sync/application (sim/node-state healed peer) :doc))
            (str peer " missed a partition contribution")))))

  (testing "a fenced single writer heals two readers without electing via discovery"
    (let [old {:writer :writer :term 2 :index 4 :root :old}
          current {:writer :writer :term 3 :index 8 :root :current}
          partitioned
          (-> (sim/make-sim {:seed 72 :latency-min 1 :latency-max 8})
              (sim/add-node :writer sync/handler
                            (sync-node :writer root-profile-id root-profile :db current))
              (sim/add-node :near sync/handler
                            (sync-node :near root-profile-id root-profile :db old))
              (sim/add-node :far sync/handler
                            (sync-node :far root-profile-id root-profile :db old))
              (sim/partition-network {:writer :left :near :left :far :right})
              (pull :near :writer :db root-profile-id :near-sync)
              (sim/send-message :app :far
                                {:type :sync/start :peer :writer :topic :db
                                 :profile root-profile-id :session :far-cut})
              (sim/run-until 25000))
          healed (-> partitioned
                     (sim/heal)
                     (pull :far :writer :db root-profile-id :far-heal))]
      (is (= current (sync/application (sim/node-state partitioned :near) :db)))
      (is (= old (sync/application (sim/node-state partitioned :far) :db)))
      (is (= :desynchronized (sync/status (sim/node-state partitioned :far) :db)))
      (doseq [peer [:writer :near :far]]
        (is (= current (sync/application (sim/node-state healed peer) :db)))))))

(deftest chunks-are-windowed-ordered-and-staged
  (testing "the responder sends no more than the acknowledged window"
    (let [server (sync-node :server join-profile-id join-profile :doc
                            (set (range 10)) {:window 2})
          result (sync/handler server
                               {:type :message :from :client
                                :payload {:type :sync/open :session :s
                                          :topic :doc :profile join-profile-id
                                          :summary #{}}}
                               {:now 0})
          chunks (filter #(= :sync/chunk (:type (nth % 2 nil)))
                         (:actions result))]
      (is (= 2 (count chunks)))))

  (testing "chunks do not mutate committed state before done"
    (let [topic "doc"
          origin (apply str (repeat 64 "0"))
          [receiver _] (sync/start
                        (sync-node :client join-profile-id join-profile topic #{:local})
                        :server topic join-profile-id :s)
          chunked (sync/handler receiver
                                {:type :message :from :server
                                 :payload {:type :sync/chunk :session :s
                                           :index 0 :data #{:remote}}}
                                {:now 1})]
      (is (= #{:local} (sync/application (:state chunked) topic)))
      (let [done (sync/handler (:state chunked)
                               {:type :message :from :server
                                :payload {:type :sync/done :session :s :count 1
                                          :checkpoint #{:remote}
                                          :frontiers {[origin 0 topic] 99}}}
                               {:now 2})]
        (is (= #{:local :remote}
               (sync/application (:state done) topic)))
        (is (empty? (sync/frontiers (:state done) topic))
            "a legacy profile must not echo a remote frontier claim")
        (is (= [[:sync-committed {}]] (:actions done))))))

  (testing "a future chunk aborts instead of applying out of order"
    (let [[receiver _] (sync/start
                        (sync-node :client join-profile-id join-profile :doc #{})
                        :server :doc join-profile-id :s)
          result (sync/handler receiver
                               {:type :message :from :server
                                :payload {:type :sync/chunk :session :s
                                          :index 1 :data #{:future}}}
                               {:now 1})]
      (is (= :sync/abort (:type (nth (first (:actions result)) 2))))
      (is (= #{} (sync/application (:state result) :doc))))))

(deftest application-receipts-not-network-admission-advance-frontiers
  (let [state (sync/make-state :reader {})
        publication {:origin :writer :epoch 0 :seq 0 :topic :doc}
        remote (sync/handler state
                             {:type :message :from :writer
                              :payload {:type :application-committed
                                        :publication publication}}
                             {:now 0})
        local (sync/handler (:state remote)
                            {:type :message :from :app
                             :payload {:type :application-committed
                                       :publication publication}}
                            {:now 1})]
    (is (empty? (sync/frontiers (:state remote) :doc))
        "a peer cannot mint our application receipt")
    (is (= {[:writer 0 :doc] 0}
           (sync/frontiers (:state local) :doc)))
    (let [gap (sync/handler (:state local)
                            {:type :message :from :app
                             :payload {:type :application-committed
                                       :publication (assoc publication :seq 2)}}
                            {:now 2})]
      (is (= {[:writer 0 :doc] 0}
             (sync/frontiers (:state gap) :doc)))
      (is (= :desynchronized (sync/status (:state gap) :doc))))))

(deftest an-opted-in-profile-mints-only-its-locally-validated-frontier
  (let [topic "doc"
        origin (apply str (repeat 64 "0"))
        frontier {[origin 0 topic] 0}
        impl
        (reify
          profile/SyncProfile
          (-profile-id [_] "test/validated-frontier/1")
          (-describe [_ value _] value)
          (-respond [_ value _ _]
            {:chunks [value]
             :checkpoint {:application value :frontiers frontier}})
          (-begin [_ value _] value)
          (-apply-chunk [_ _ _ chunk] chunk)
          (-commit [_ _ _ staging _] staging)

          profile/CommittedFrontierProfile
          (-commit-with-frontiers [_ _ _ staging checkpoint claimed]
            (when-not (= claimed (:frontiers checkpoint))
              (throw (ex-info "frontier was not bound to checkpoint" {})))
            {:application staging
             :frontiers claimed}))
        profile-id (profile/profile-id impl)
        [receiver _] (sync/start
                      (sync-node :client profile-id impl topic :old)
                      :server topic profile-id :verified)
        chunked (sync/handler receiver
                              {:type :message :from :server
                               :payload {:type :sync/chunk :session :verified
                                         :index 0 :data :new}}
                              {:now 1})
        done (sync/handler (:state chunked)
                           {:type :message :from :server
                            :payload {:type :sync/done :session :verified
                                      :count 1
                                      :topic topic
                                      :checkpoint {:application :new
                                                   :frontiers frontier}
                                      :frontiers frontier}}
                           {:now 2})]
    (is (= :new (sync/application (:state done) topic)))
    (is (= frontier (sync/frontiers (:state done) topic)))
    (is (= [[:sync-committed frontier]] (:actions done)))))

(deftest sender-frontier-claims-are-validated-before-profile-code
  (let [commits (atom 0)
        topic "doc"
        impl (reify
               profile/SyncProfile
               (-profile-id [_] "test/precommit-validation/1")
               (-describe [_ value _] value)
               (-respond [_ _ _ _] {:chunks [] :checkpoint nil})
               (-begin [_ value _] value)
               (-apply-chunk [_ staging _ _] staging)
               (-commit [_ _ _ staging _] (swap! commits inc) staging))
        profile-id (profile/profile-id impl)
        [receiver _] (sync/start
                      (sync-node :client profile-id impl topic :old)
                      :server topic profile-id :bad-claim)
        result (sync/handler
                receiver
                {:type :message :from :server
                 :payload {:type :sync/done :session :bad-claim :count 0
                           :topic topic :checkpoint nil
                           :frontiers [{"origin" (identity/hex->bytes
                                                  (apply str (repeat 62 "0")))
                                        "epoch" 0 "topic" topic "through" 0}]}}
                {:now 1})]
    (is (zero? @commits))
    (is (= :sync/abort
           (:type (nth (first (:actions result)) 2))))))

(deftest a-lost-session-times-out-desynchronized
  (let [s (-> (sim/make-sim {:drop-p 1.0})
              (sim/add-node :client sync/handler
                            (sync-node :client join-profile-id join-profile :doc #{}))
              (sim/add-node :server sync/handler
                            (sync-node :server join-profile-id join-profile :doc #{:x}))
              (sim/send-message :app :client
                                {:type :sync/start :peer :server :topic :doc
                                 :profile join-profile-id :session :lost})
              (sim/run-until 20000))]
    (is (= :desynchronized (sync/status (sim/node-state s :client) :doc)))
    (is (empty? (:sessions (sim/node-state s :client))))))

(deftest legacy-overlay-commit-does-not-trust-a-transport-frontier
  (let [old {:writer :writer :term 1 :index 1 :root :old}
        current {:writer :writer :term 2 :index 2 :root :current}
        opts (fn [value]
               {:topics #{:db}
                :sync {:profiles {root-profile-id root-profile}
                       :applications {:db value}}})
        later {:type :gossip :topic :db :origin :writer :epoch 0
               :seq 1 :hops 0 :payload :live-after-snapshot}
        s (-> (sim/make-sim {:seed 4 :latency-min 1 :latency-max 5})
              (sim/add-node :reader overlay/handler
                            (overlay/make-state :reader (opts old)))
              (sim/add-node :writer overlay/handler
                            (overlay/make-state :writer (opts current)))
              ;; Establish the responder's stream frontier at zero without
              ;; requiring a membership connection in this focused fixture.
              (sim/send-message :app :writer
                                {:type :publish :topic :db :payload :snapshot-live})
              (sim/run-until 100)
              ;; Sequence one arrives first and must remain behind the missing
              ;; prefix until the snapshot says it includes sequence zero.
              (sim/send-message :writer :reader later)
              (sim/run-until 200)
              (sim/send-message :app :reader
                                {:type :sync/start :peer :writer :topic :db
                                 :profile root-profile-id :session :overlay-sync})
              (sim/run-until 2000))
        reader (sim/node-state s :reader)]
    (is (= current (overlay/synced-value reader :db)))
    (is (empty? (overlay/delivered reader))
        "a legacy profile cannot suppress the missing live prefix")
    (is (= 0 (get-in reader [:dissemination :delivery-next
                             [:writer 0 :db]] 0)))
    (is (= #{1}
           (set (keys (get-in reader [:dissemination :delivery-pending
                                      [:writer 0 :db]])))))))
