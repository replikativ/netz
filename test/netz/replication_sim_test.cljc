(ns netz.replication-sim-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.replication :as replication]
            [netz.sim :as sim]))

(def alice-id (apply str (repeat 32 "aa")))
(def bob-id (apply str (repeat 32 "bb")))
(def carol-id (apply str (repeat 32 "cc")))

(def layout
  {:epoch 1
   :members [{:id alice-id
              :domains {"owner" "alice" "site" "alice-home"}}
             {:id bob-id
              :domains {"owner" "bob" "site" "bob-home"}}
             {:id carol-id
              :domains {"owner" "carol" "site" "carol-home"}}]})

(def policy
  {:replicas 3
   :write-acks 2
   :minimum-domains {"owner" 2 "site" 2}})

(defn- controller-actions [state event context]
  (let [{controller :state actions :actions}
        (replication/handler (:controller state) event context)]
    {:state (assoc state :controller controller)
     :actions
     (mapv (fn [[op target manifest transfer :as action]]
             (case op
               :persist-intent
               [:persist [:replication/intent target] manifest
                {:op :intent-stored
                 :object-id target
                 :layout-epoch (:layout-epoch manifest)}]

               :store
               [:send target {:op :store
                              :manifest manifest
                              :transfer transfer}]

               [:record action]))
           actions)}))

(defn controller-handler [state event context]
  (cond
    (= :init (:type event))
    (reduce (fn [{current :state accumulated :actions} object-id]
              (let [{next-state :state actions :actions}
                    (controller-actions current
                                        {:type :replication/repair
                                         :object-id object-id}
                                        context)]
                {:state next-state :actions (into accumulated actions)}))
            {:state state :actions []}
            (keys (get-in state [:controller :objects])))

    (= :persist-result (:type event))
    (if (= :ok (:result event))
      (controller-actions state
                          (assoc (:payload event)
                                 :type :replication/intent-stored)
                          context)
      {:state (update state :records conj
                      [:intent-failed {:result (:result event)}])
       :actions []})

    (= :message (:type event))
    (let [payload (:payload event)]
      (case (:op payload)
        :begin (controller-actions state
                                   {:type :replication/begin
                                    :manifest (:manifest payload)} context)
        :stored (controller-actions state
                                    (assoc (:transfer payload)
                                           :type :replication/stored
                                           :object-id (:object-id payload)
                                           :replica-id (:from event)) context)
        :lost (controller-actions state
                                  {:type :replication/lost
                                   :object-id (:object-id payload)
                                   :replica-id (:from event)
                                   :layout-epoch (:layout-epoch payload)} context)
        :repair (controller-actions state
                                    {:type :replication/repair
                                     :object-id (:object-id payload)} context)
        {:state state :actions []}))

    :else
    {:state state :actions []}))

(defn replica-handler [state event _context]
  (case (:type event)
    :message
    (let [{:keys [op manifest transfer]} (:payload event)]
      (if (= :store op)
        {:state state
         :actions [[:persist [:blob (:object-id manifest)] manifest
                    {:coordinator (:from event)
                     :object-id (:object-id manifest)
                     :transfer transfer}]]}
        {:state state :actions []}))

    :persist-result
    (let [{:keys [coordinator object-id transfer]} (:payload event)]
      (if (= :ok (:result event))
        {:state (assoc-in state [:installed object-id]
                          {:coordinator coordinator
                           :layout-epoch (:layout-epoch transfer)})
         :actions [[:send coordinator {:op :stored
                                       :object-id object-id
                                       :transfer transfer}]]}
        {:state (update state :failures conj
                        {:object-id object-id :result (:result event)})
         :actions []}))

    :disk-lost
    {:state (assoc state :installed {})
     :actions (mapv (fn [[object-id {:keys [coordinator layout-epoch]}]]
                      [:send coordinator {:op :lost
                                          :object-id object-id
                                          :layout-epoch layout-epoch}])
                    (:installed state))}

    :disk-corrupt
    ;; A scrub would discover this in a real runtime. The simulator supplies a
    ;; deterministic notification so repair logic can be tested independently
    ;; of a particular byte representation.
    (let [[_ object-id] (:key event)
          {:keys [coordinator layout-epoch]} (get-in state [:installed object-id])]
      {:state (update state :installed dissoc object-id)
       :actions (if coordinator
                  [[:send coordinator {:op :lost
                                       :object-id object-id
                                       :layout-epoch layout-epoch}]]
                  [])})

    {:state state :actions []}))

(defn- cluster []
  (reduce (fn [s id]
            (sim/add-node s id replica-handler {:installed {} :failures []}))
          (sim/add-node
           (sim/make-sim {:seed 22 :latency-min 1 :latency-max 4
                          :persist-latency-ms 3})
           "controller" controller-handler
           {:controller (replication/make-state layout policy)
            :records []}
           {:restore
            (fn [initial durable]
              (reduce-kv
               (fn [state key entry]
                 (if (and (vector? key) (= :replication/intent (first key)))
                   (-> state
                       (assoc-in [:controller :objects (second key)]
                                 (assoc entry :intent-stored? true))
                       (update-in [:controller :stats :intents-stored] inc))
                   state))
               initial durable))})
          (map :id (:members layout))))

(defn- controller-state [simulation]
  (get-in (sim/node-state simulation "controller") [:controller]))

(defn- records [simulation]
  (:records (sim/node-state simulation "controller")))

;; `:record` is a test-runtime action, kept outside the generic simulator's
;; protocol vocabulary. Adapt it into application delivery so the generic
;; simulator can count it and the controller can retain an inspectable log.
(defn- recording-controller [state event context]
  (let [{:keys [state actions]} (controller-handler state event context)
        records (filterv #(= :record (first %)) actions)]
    {:state (update state :records into (map second records))
     :actions (filterv #(not= :record (first %)) actions)}))

(defn- recording-cluster []
  (assoc-in (cluster) [:nodes "controller" :handler] recording-controller))

(deftest three-private-devices-reach-quorum-and-repair-disk-loss
  (let [manifest {:object-id "encrypted-photo" :length 4096}
        complete (-> (recording-cluster)
                     (sim/send-message "client" "controller"
                                       {:op :begin :manifest manifest})
                     (sim/run-until 100))
        target (first (get-in (controller-state complete)
                              [:objects "encrypted-photo" :targets]))]
    (is (= [:durable :complete] (mapv first (records complete))))
    (doseq [id (map :id (:members layout))]
      (is (= manifest (get (sim/durable-state complete id)
                           [:blob "encrypted-photo"]))))

    (testing "loss of one physical device is detected and repaired"
      (let [repaired (-> complete
                         (sim/lose-disk target)
                         (sim/run-until 200))]
        (is (= manifest (get (sim/durable-state repaired target)
                             [:blob "encrypted-photo"])))
        (is (= [:durable :complete :repaired]
               (mapv first (records repaired))))
        (is (= 1 (get-in (controller-state repaired) [:stats :repaired])))))))

(deftest partition-can-reach-w-but-not-falsely-claim-n
  (let [isolated carol-id
        manifest {:object-id "ledger" :length 100}
        partitioned (-> (recording-cluster)
                        (sim/partition-network
                         {"controller" :main
                          alice-id :main bob-id :main
                          isolated :remote})
                        (sim/send-message "client" "controller"
                                          {:op :begin :manifest manifest})
                        (sim/run-until 100))]
    (is (= [:durable] (mapv first (records partitioned))))
    (is (nil? (get (sim/durable-state partitioned isolated) [:blob "ledger"])))

    (testing "an explicit repair scan after healing reaches the missing site"
      (let [healed (-> partitioned
                       (sim/heal)
                       (sim/send-message "operator" "controller"
                                         {:op :repair :object-id "ledger"})
                       (sim/run-until 200))]
        (is (= manifest (get (sim/durable-state healed isolated)
                             [:blob "ledger"])))
        (is (= [:durable :complete] (mapv first (records healed))))))))

(deftest failed-disks-cannot-manufacture-a-quorum-receipt
  (let [failed (-> (recording-cluster)
                   (sim/set-disk-writable bob-id false)
                   (sim/set-disk-writable carol-id false)
                   (sim/send-message "client" "controller"
                                     {:op :begin
                                      :manifest {:object-id "only-local"}})
                   (sim/run-until 100))]
    (is (empty? (records failed)))
    (is (= 1 (count (get-in (controller-state failed)
                            [:objects "only-local" :acks]))))
    (is (= #{:write-failed}
           (set (mapcat (fn [id]
                          (map :result (:failures (sim/node-state failed id))))
                        [bob-id carol-id]))))))

(deftest durable-outbox-recovers-the-crash-between-intent-and-receipts
  (let [manifest {:object-id "crash-window" :length 50}
        interrupted (-> (recording-cluster)
                        (sim/send-message "client" "controller"
                                          {:op :begin :manifest manifest})
                        ;; The intent lands at t=3. Remote stores are still in
                        ;; flight, and their acknowledgements will be lost when
                        ;; the coordinator crashes.
                        (sim/run-until 3)
                        (sim/crash "controller"))]
    (is (contains? (sim/durable-state interrupted "controller")
                   [:replication/intent "crash-window"]))
    (let [recovered (-> interrupted
                        (sim/restart "controller")
                        (sim/run-until 100))]
      (is (= [:durable :complete] (mapv first (records recovered))))
      (is (= 3 (count (get-in (controller-state recovered)
                              [:objects "crash-window" :acks])))))))

(deftest failed-outbox-prevents-all-network-storage
  (let [failed (-> (recording-cluster)
                   (sim/set-disk-writable "controller" false)
                   (sim/send-message "client" "controller"
                                     {:op :begin
                                      :manifest {:object-id "not-durable"}})
                   (sim/run-until 100))]
    (is (= [:intent-failed] (mapv first (records failed))))
    (doseq [id (map :id (:members layout))]
      (is (empty? (sim/durable-state failed id))))))
