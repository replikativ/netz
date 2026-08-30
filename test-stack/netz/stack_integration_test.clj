(ns netz.stack-integration-test
  "Workspace integration proofs against the real sibling libraries.

  This suite deliberately lives behind the `:stack` alias. None of these
  dependencies leaks into Netz's artifact; the tests are the executable seam
  between independently releasable projects."
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is testing]]
            [datahike.api :as datahike]
            [netz.sim :as sim]
            [netz.sync :as sync]
            [netz.sync.profile :as profile]
            [netz.sync.root-log :as root-log]
            [org.replikativ.spindel.ygg-signal :as ygg-signal]
            [urd.consensus.algorithms.raft :as raft]
            [urd.consensus.sim :as urd-sim]
            [yggdrasil.convergent.gset :as gset]))

(defn- sync-node [id profile-id impl topic value & [opts]]
  (sync/make-state id
                   (merge {:profiles {profile-id impl}
                           :applications {topic value}}
                          opts)))

(defn- pull [simulation requester responder topic profile-id session]
  (-> simulation
      (sim/send-message :app requester
                        {:type :sync/start :peer responder :topic topic
                         :profile profile-id :session session})
      (sim/run-until (+ (:now simulation) 20000))))

(defn- raft-fence []
  (let [peers [:urd/a :urd/b :urd/c]
        node-opts {:election-timeout 10
                   :heartbeat-timeout 3
                   :propose-interval 2}
        simulation
        (urd-sim/run
         (urd-sim/make
          {:seed 3
           :nodes (zipmap peers (repeat (raft/make)))
           :node-config (zipmap peers (repeat node-opts))
           :config {:min-delay 1
                    :max-delay 3
                    :persist-latency 2
                    :value-fn (fn [state peer _]
                                {:proposed-at (:tick state) :by peer})}})
         300)
        leaders (filterv #(and (not (contains? (:crashed simulation) %))
                               (= :leader (:role (urd-sim/node-state simulation %))))
                         peers)
        leader (first leaders)
        leader-state (urd-sim/node-state simulation leader)]
    (is (= 1 (count leaders)) "Urd must select exactly one live writer")
    (is (pos? (:commit leader-state)) "the fence must cover committed work")
    {:writer leader
     :term (:term leader-state)
     :index (:commit leader-state)}))

(deftest urd-fences-a-real-datahike-root-through-netz
  (let [config {:store {:backend :memory :id (random-uuid)}
                :schema-flexibility :write
                :keep-history? true}]
    (datahike/create-database config)
    (let [connection (datahike/connect config)]
      (try
        (datahike/transact connection
                           [{:db/ident :task/title
                             :db/valueType :db.type/string
                             :db/cardinality :db.cardinality/one}])
        (datahike/transact connection [{:task/title "ship Netz"}])
        (let [root (get-in @connection [:meta :datahike/commit-id])
              fence (raft-fence)
              record (assoc fence :root root)
              impl (root-log/profile (:writer fence) #(uuid? (:root %)))
              profile-id (profile/profile-id impl)
              simulation
              (-> (sim/make-sim {:seed 31 :latency-min 1 :latency-max 7})
                  (sim/add-node :reader sync/handler
                                (sync-node :reader profile-id impl :datahike nil))
                  (sim/add-node :writer sync/handler
                                (sync-node :writer profile-id impl :datahike record))
                  (pull :reader :writer :datahike profile-id :datahike-root))
              received (sync/application (sim/node-state simulation :reader)
                                         :datahike)]
          (is (uuid? root) "Datahike must expose a content-addressed commit root")
          (is (= record received)
              "Netz transfers the root under the exact Urd term/index fence")
          (is (= 1 (datahike/q '[:find (count ?e) .
                                :where [?e :task/title _]] @connection)))
          (testing "discovery or a claimed publisher cannot bypass consensus"
            (is (thrown? Exception
                         (profile/apply-chunk
                          impl nil :datahike
                          (assoc record :writer :unselected-peer))))))
        (finally
          (datahike/release connection)
          (datahike/delete-database config))))))

(def spindel-gset-profile-id "stack/spindel-yggdrasil-gset/1")

(def spindel-gset-profile
  (reify profile/SyncProfile
    (-profile-id [_] spindel-gset-profile-id)
    (-describe [_ value _topic]
      (gset/elements value))
    (-respond [_ value _topic remote-summary]
      (let [missing (set/difference (gset/elements value) (set remote-summary))]
        {:chunks (cond-> [] (seq missing) (conj missing))
         :checkpoint (gset/elements value)}))
    (-begin [_ value _topic] value)
    (-apply-chunk [_ staging _topic chunk]
      ;; This is Spindel's production OP-path hook, backed by Yggdrasil's
      ;; PDeltaApply implementation rather than a Netz-specific set union.
      (ygg-signal/ygg-apply-delta-fn staging chunk))
    (-commit [_ _value _topic staging checkpoint]
      (when-not (set/subset? (set checkpoint) (gset/elements staging))
        (throw (ex-info "Yggdrasil checkpoint was not fully staged"
                        {:checkpoint checkpoint})))
      staging)))

(defn- memory-gset [id]
  (gset/gset id {:store-config {:backend :memory :id (random-uuid)}}))

(deftest spindel-yggdrasil-deltas-heal-through-netz
  (let [a (-> (memory-gset "netz-stack-a")
              (gset/conj :from-a))
        b (-> (memory-gset "netz-stack-b")
              (gset/conj :from-b))
        partitioned
        (-> (sim/make-sim {:seed 41 :latency-min 1 :latency-max 8})
            (sim/add-node :a sync/handler
                          (sync-node :a spindel-gset-profile-id
                                     spindel-gset-profile :doc a))
            (sim/add-node :b sync/handler
                          (sync-node :b spindel-gset-profile-id
                                     spindel-gset-profile :doc b))
            (sim/partition-network {:a :left :b :right})
            (sim/send-message :app :a
                              {:type :sync/start :peer :b :topic :doc
                               :profile spindel-gset-profile-id
                               :session :partitioned})
            (sim/run-until 20000))]
    (is (= :desynchronized (sync/status (sim/node-state partitioned :a) :doc)))
    (let [healed (-> partitioned
                     (sim/heal)
                     (pull :a :b :doc spindel-gset-profile-id :a-pulls-b)
                     (pull :b :a :doc spindel-gset-profile-id :b-pulls-a))]
      (doseq [peer [:a :b]]
        (is (= #{:from-a :from-b}
               (gset/elements
                (sync/application (sim/node-state healed peer) :doc)))
            (str peer " did not converge through the Spindel delta hook"))))
    (testing "Spindel derives both state and delta synchronization hooks"
      (let [opts (ygg-signal/sync-opts a :sync? true)]
        (is (fn? (:merge-fn opts)))
        (is (fn? (:apply-delta-fn opts)))
        (is (fn? (:delta-fn opts)))
        (is (fn? (:clear-delta-fn opts)))))))
