(ns netz.sync-test
  (:require [clojure.test :refer [deftest is testing]]
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
  (let [old {:writer :writer :term 1 :index 4 :root :old}
        current {:writer :writer :term 2 :index 7 :root :current}
        s (-> (sim/make-sim {:seed 8 :latency-min 1 :latency-max 10})
              (sim/add-node :reader sync/handler
                            (sync-node :reader root-profile-id root-profile :db old))
              (sim/add-node :writer sync/handler
                            (sync-node :writer root-profile-id root-profile :db current
                                       {:frontiers {:db {[:writer 0 :db] 7}}}))
              (pull :reader :writer :db root-profile-id :root-session))
        reader (sim/node-state s :reader)]
    (is (= current (sync/application reader :db)))
    (is (= :synchronized (sync/status reader :db)))
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
    (let [[receiver _] (sync/start
                        (sync-node :client join-profile-id join-profile :doc #{:local})
                        :server :doc join-profile-id :s)
          chunked (sync/handler receiver
                                {:type :message :from :server
                                 :payload {:type :sync/chunk :session :s
                                           :index 0 :data #{:remote}}}
                                {:now 1})]
      (is (= #{:local} (sync/application (:state chunked) :doc)))
      (let [done (sync/handler (:state chunked)
                               {:type :message :from :server
                                :payload {:type :sync/done :session :s :count 1
                                          :checkpoint #{:remote}
                                          :frontiers {}}}
                               {:now 2})]
        (is (= #{:local :remote}
               (sync/application (:state done) :doc))))))

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

(deftest overlay-commit-advances-ordered-live-frontier
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
    (is (= [:live-after-snapshot] (overlay/delivered reader)))
    (is (= 2 (get-in reader [:dissemination :delivery-next
                             [:writer 0 :db]])))))
