(ns netz.sim-portability-test
  (:require [clojure.test :refer [deftest is]]
            [netz.sim :as sim]
            [netz.sim.schedule :as schedule]))

(defn fingerprint-handler [state event _ctx]
  (case (:type event)
    :init {:state state :actions []}
    :message (let [{:keys [id value] :as payload} (:payload event)]
               (if (contains? (:seen state) id)
                 {:state (update state :duplicates inc) :actions []}
                 {:state (-> state
                             (update :seen conj id)
                             (update :received conj value))
                  :actions (vec (for [peer (:peers state)
                                      :when (not= peer (:from event))]
                                  [:send peer payload]))}))
    {:state state :actions []}))

(defn fingerprint-network []
  (reduce (fn [s i]
            (let [id (keyword (str "n" i))
                  left (keyword (str "n" (mod (dec i) 4)))
                  right (keyword (str "n" (mod (inc i) 4)))]
              (sim/add-node s id fingerprint-handler
                            {:peers [left right]
                             :seen #{} :received [] :duplicates 0})))
          (sim/make-sim {:seed 23 :latency-min 1 :latency-max 9
                         :drop-p 0.1 :duplicate-p 0.25 :trace? false})
          (range 4)))

(def fingerprint-schedule
  [[:inject :external :n0 {:id 1 :value :one}]
   [:advance 100]
   [:partition {:n0 :left :n1 :left :n2 :right :n3 :right}]
   [:inject :external :n0 {:id 2 :value :partitioned}]
   [:advance 100]
   [:heal]
   [:inject :external :n3 {:id 3 :value :three}]
   [:advance 1000]])

(defn simulation-fingerprint []
  (let [s (schedule/run (fingerprint-network) fingerprint-schedule)]
    {:now (:now s)
     :steps (:steps s)
     :stats (:stats s)
     :bytes-sent (into (sorted-map) (:bytes-sent s))
     :peak-time-bytes (into (sorted-map) (:peak-time-bytes s))
     :peak-inflight (:peak-inflight s)
     :nodes (into (sorted-map)
                  (for [id (sort (sim/node-ids s))
                        :let [state (sim/node-state s id)]]
                    [id (select-keys state [:seen :received :duplicates])]))}))

(def expected-fingerprint
  {:now 1200
   :steps 20
   :stats {:connects 0 :dropped-loss 0 :dropped-partition 2
           :delivered 16 :sent 13 :timers 0 :persisted 0 :dropped-down 0
           :persist-attempts 0 :dropped-overflow 0 :duplicated 2
           :dropped-bandwidth 0}
   :bytes-sent {:n0 114 :n1 54 :n2 54 :n3 82}
   :peak-time-bytes {:n0 52 :n1 28 :n2 28 :n3 56}
   :peak-inflight 3
   :nodes {:n0 {:seen #{1 2 3}
                :received [:one :partitioned :three] :duplicates 0}
           :n1 {:seen #{1 2 3}
                :received [:one :partitioned :three] :duplicates 3}
           :n2 {:seen #{1 3} :received [:one :three] :duplicates 3}
           :n3 {:seen #{1 3} :received [:one :three] :duplicates 0}}})

(deftest whole-simulation-is-portable
  (is (= expected-fingerprint (simulation-fingerprint))))
