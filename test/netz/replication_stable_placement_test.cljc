(ns netz.replication-stable-placement-test
  (:require [clojure.test :refer [deftest is]]
            [netz.identity :as identity]
            [netz.replication.placement :as placement]))

(def seed (apply str (repeat 64 "a")))
(def members
  (mapv (fn [s] {:id (apply str (repeat 64 s))
                 :domains {"site" s}})
        ["1" "2" "3" "4" "5" "6" "7" "8"]))
(def layout {:epoch 0 :members members :placement "netz/blob-placement/2" :seed seed})
(def policy {:replicas 3 :write-acks 2})
(defn targets [layout object]
  (set (map :id (placement/place layout policy object))))

(deftest stable-placement-is-independent-of-control-epoch-and-member-order
  (doseq [object (map #(str "object-" %) (range 200))]
    (is (= (targets layout object)
           (targets (assoc layout :epoch 1) object)
           (targets (update layout :members #(vec (reverse %))) object)))))

(deftest unconstrained-membership-changes-have-minimal-movement
  (let [removed (:id (first members))
        smaller (assoc layout :epoch 1 :members (subvec members 1))]
    (doseq [object (map #(str "object-" %) (range 200))]
      (let [before (targets layout object)
            after (targets smaller object)]
        (is (= 3 (count after)))
        (is (every? after (disj before removed)))
        (when-not (contains? before removed)
          (is (= before after)))
        ;; The reverse operation adds this member: any displacement must be
        ;; caused by that member entering the preferred set.
        (is (or (= before after) (contains? before removed)))))))

(deftest stable-placement-respects-domains-and-disabled-members
  (let [ranked (placement/rank layout "domains")
        same-site (set (map :id (take 3 ranked)))
        constrained (update layout :members
                            #(mapv (fn [m] (if (same-site (:id m))
                                             (assoc m :domains {"site" "same"})
                                             m)) %))
        selected (placement/place constrained
                                  (assoc policy :minimum-domains {"site" 2})
                                  "domains")]
    (is (placement/satisfies-domains? selected {"site" 2}))
    (is (= (mapv #(apply str (repeat 64 %)) ["5" "6" "1"])
           (mapv :id selected))))
  (is (not (contains? (targets (assoc-in layout [:members 0 :enabled?] false)
                               "disabled")
                      (:id (first members)))))
  (is (try (placement/rank (assoc layout :seed "bad-seed") "x") false
           (catch #?(:clj Exception :cljs :default) _ true))))

(deftest portable-score-vector
  (is (= "93aee44dcdbfc81b052c2c17a3e396ccb76bfa2ff62b52b8ee0691269cf1ac65"
         (identity/bytes->hex (placement/score seed "object-0" (:id (first members)))))))
