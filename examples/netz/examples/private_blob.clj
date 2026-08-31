(ns netz.examples.private-blob
  "Small executable walkthrough of private three-device durability semantics."
  (:require [netz.identity :as identity]
            [netz.replication :as replication]
            [netz.replication.blob :as blob]))

(def devices
  [{:id (apply str (repeat 32 "aa"))
    :label "Alice's home Pi"
    :domains {"owner" "alice" "site" "alice-home"}}
   {:id (apply str (repeat 32 "bb"))
    :label "Bob's home Pi"
    :domains {"owner" "bob" "site" "bob-home"}}
   {:id (apply str (repeat 32 "cc"))
    :label "Carol's mini-PC"
    :domains {"owner" "carol" "site" "carol-home"}}])

(def labels (into {} (map (juxt :id :label)) devices))

(defn- advance [state event]
  (replication/handler state event {:now 0}))

(defn- stored-event [object-id [op target _manifest transfer]]
  (assert (= :store op))
  (assoc transfer
         :type :replication/stored
         :object-id object-id
         :replica-id target))

(defn -main [& _]
  ;; This represents randomized AEAD ciphertext at the network boundary. The
  ;; demonstration deliberately never gives storage-only replicas a key.
  (let [ciphertext (identity/seq->bytes (range 256))
        split (blob/split ciphertext {:piece-size 64})
        object-id (identity/bytes->hex (:object-id split))
        manifest {:object-id object-id
                  :transfer-manifest (:manifest split)
                  :privacy-domain "friends-private-demo"}
        initial (replication/make-state
                 {:epoch 1 :members (mapv #(dissoc % :label) devices)}
                 {:replicas 3 :write-acks 2
                  :minimum-domains {"owner" 2 "site" 2}})
        {intent-state :state intent-actions :actions}
        (advance initial {:type :replication/begin :manifest manifest})
        {ready :state stores :actions}
        (advance intent-state {:type :replication/intent-stored
                               :object-id object-id :layout-epoch 1})
        first-copy (advance ready (stored-event object-id (nth stores 0)))
        quorum (advance (:state first-copy)
                        (stored-event object-id (nth stores 1)))
        complete (advance (:state quorum)
                          (stored-event object-id (nth stores 2)))
        lost-target (second (nth stores 0))
        {degraded :state repair :actions}
        (advance (:state complete)
                 {:type :replication/lost :object-id object-id
                  :replica-id lost-target :layout-epoch 1})
        repaired (advance degraded (stored-event object-id (first repair)))]
    (println "Private three-device blob demonstration")
    (prn {:object object-id
          :pieces (count (:pieces split))
          :outbox-action (ffirst intent-actions)
          :targets (mapv #(labels (second %)) stores)})
    (prn {:after-two-copies (:actions quorum)})
    (prn {:after-three-copies (:actions complete)})
    (prn {:lost (labels lost-target)
          :repair-attempt (get-in repair [0 3 :attempt])
          :after-repair (:actions repaired)})))

