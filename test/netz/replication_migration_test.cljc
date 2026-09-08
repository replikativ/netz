(ns netz.replication-migration-test
  (:require [clojure.test :refer [deftest is]]
            [netz.replication.migration :as migration]))

(defn id [s] (apply str (repeat 64 s)))
(defn initial []
  (migration/create
   {:binding {:cluster "pool" :object "blob" :old-layout "old-address"
              :new-layout "new-address" :controller "fencing-token"}
    :layout {:epoch 1 :seed (id "a") :placement "netz/blob-placement/2"
             :members (mapv #(hash-map :id (id %) :domains {"site" %})
                            ["1" "2" "3"])}
    :policy {:replicas 3 :write-acks 2 :minimum-domains {"site" 2}}
    :old-holders #{(id "0") (id "1")} :retire-after 10}))
(defn persist [s]
  (:state (migration/step s {:type :persisted :checkpoint (migration/checkpoint s)})))
(defn receipt [s replica]
  {:type :receipt :binding (:binding s) :replica replica
   :attempt (get-in s [:attempts replica]) :expires 100})
(defn candidates [s]
  (migration/retirement-candidates s {:binding (:binding s) :now 20
                                      :protected-holders #{}}))
(defn fill [s]
  (reduce (fn [s replica] (persist (:state (migration/step s (receipt s replica)))))
          (persist s) (keys (:attempts s))))

(deftest copy-planning-requires-current-durable-transition
  (let [initial (initial)
        state (persist initial)
        context {:binding (:binding state) :now 20}
        work (migration/copy-candidates state context)
        replica (:replica (first work))
        pending (:state (migration/step state (receipt state replica)))
        accepted (persist pending)]
    (is (empty? (migration/copy-candidates initial context)))
    (is (= (mapv :id (:targets state)) (mapv :replica work)))
    (is (every? #(= {:binding (:binding state) :replica (:replica %) :attempt 0} %) work))
    (is (empty? (migration/copy-candidates pending context)))
    (is (= (vec (rest work)) (migration/copy-candidates accepted context)))
    (is (= work (migration/copy-candidates accepted (assoc context :now 100))))
    (doseq [invalid [(assoc context :now -1) (assoc context :now nil)
                     (assoc-in context [:binding :controller] "stale")]]
      (is (empty? (migration/copy-candidates state invalid))))
    (is (empty? (migration/copy-candidates
                 (persist (:state (migration/step state {:type :halt}))) context)))))

(deftest copy-planning-replays-exact-attempt-after-loss
  (let [state (fill (initial))
        replica (:id (first (:targets state)))
        context {:binding (:binding state) :now 20}
        lost (:state (migration/step state (assoc (receipt state replica) :type :lost)))]
    (is (empty? (migration/copy-candidates state context)))
    (is (empty? (migration/copy-candidates lost context)))
    (is (= [{:binding (:binding state) :replica replica :attempt 1}]
           (migration/copy-candidates (persist lost) context)))
    (is (= (migration/copy-candidates (persist lost) context)
           (migration/copy-candidates (migration/restore (migration/checkpoint lost)) context)))))

(deftest all-n-and-persistence-before-retirement
  (let [s (persist (initial)) ids (vec (keys (:attempts s)))]
    (loop [s s remaining ids]
      (when-let [replica (first remaining)]
        (let [next (:state (migration/step s (receipt s replica)))]
          (is (empty? (candidates s)))
          (is (empty? (candidates next)))
          (is (= :busy (:status (migration/step next (receipt next replica)))))
          (if (seq (rest remaining))
            (recur (persist next) (rest remaining))
            (is (= #{(id "0")} (candidates (persist next))))))))))

(deftest stale-evidence-loss-expiry-pins-and-fencing
  (let [s (fill (initial)) replica (first (keys (:attempts s)))
        old (receipt s replica)
        lost (persist (:state (migration/step s (assoc old :type :lost))))]
    (is (empty? (candidates lost)))
    (is (= :stale (:status (migration/step lost old))))
    (is (= :stale (:status (migration/step s (assoc-in old [:binding :new-layout] "other")))))
    (doseq [context [{:now 100} {:now 9} {:protected-holders #{(id "0")}}
                     {:binding (assoc (:binding s) :controller "other")}]]
      (is (empty? (migration/retirement-candidates
                   s (merge {:binding (:binding s) :now 20 :protected-holders #{}}
                            context)))))
    (let [halted (:state (migration/step s {:type :halt}))]
      (is (empty? (candidates halted)))
      (is (empty? (candidates (persist halted))))
      (is (= :stale (:status (migration/step halted
                                             {:type :persisted :checkpoint (migration/checkpoint s)})))))))

(deftest crash-replay-at-each-receipt-boundary
  ;; Crash before write, after write before completion, or after completion.
  ;; Read back the durable checkpoint, replay duplicate messages, then converge.
  (doseq [crash-at (range 3) boundary [:before-write :after-write :after-completion]]
    (let [start (persist (initial)) ids (vec (keys (:attempts start)))]
      (loop [s start disk (migration/checkpoint start) index 0]
        (if (= index 3)
          (is (= #{(id "0")} (candidates s)))
          (let [event (receipt s (nth ids index))
                proposed (:state (migration/step s event))
                disk-next (migration/checkpoint proposed)
                recovered (if (= index crash-at)
                            (migration/restore (if (= boundary :before-write) disk disk-next))
                            (persist proposed))
                replayed (persist (:state (migration/step recovered event)))]
            (when (< index 2) (is (empty? (candidates replayed))))
            (recur replayed (migration/checkpoint replayed) (inc index))))))))

(deftest reordered-duplicates-and-each-replica-loss
  (let [start (persist (initial))
        ids (vec (keys (:attempts start)))]
    (doseq [order [[0 1 2] [0 2 1] [1 0 2] [1 2 0] [2 0 1] [2 1 0]]]
      (let [complete
            (reduce (fn [s index]
                      (let [event (receipt s (nth ids index))
                            next (persist (:state (migration/step s event)))]
                        (is (= :duplicate (:status (migration/step next event))))
                        (is (= next (:state (migration/step next event))))
                        next)) start order)]
        (is (= #{(id "0")} (candidates complete)))
        (doseq [replica ids]
          (let [old-receipt (receipt complete replica)
                lost (:state (migration/step complete (assoc old-receipt :type :lost)))
                recovered (migration/restore (migration/checkpoint lost))]
            (is (empty? (candidates lost)))
            (is (empty? (candidates recovered)))
            (is (= :stale (:status (migration/step recovered old-receipt))))
            (is (= #{(id "0")}
                   (candidates (persist (:state (migration/step recovered
                                                                (receipt recovered replica)))))))))))))
