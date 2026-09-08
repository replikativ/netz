(ns netz.replication-layout-v2-test
  (:require [clojure.test :refer [deftest is]]
            [netz.identity :as identity]
            [netz.replication.layout :as layout]
            [netz.replication.layout-v2 :as v2]))

(defn bytes32 [n] (identity/seq->bytes (repeat 32 n)))
(defn statement []
  {"v" 2 "type" "blob/layout" "cluster" (identity/seq->bytes [1])
   "epoch" 0 "previous" (bytes32 0) "issued_at" 1 "signer" (bytes32 9)
   "placement" v2/placement-profile "seed" (bytes32 10)
   "members" (mapv (fn [n] {"authority" (bytes32 n) "domains" {"site" (str n)}
                            "enabled" true}) [1 2 3 4])})

(deftest exact-versioned-schema-remains-outside-v1
  (let [s (statement)
        v1 (-> s (dissoc "placement" "seed") (assoc "v" 1))]
    (is (v2/valid-statement? s))
    (is (layout/valid-statement? v1))
    (is (not (layout/valid-statement? s)))
    (is (not (v2/valid-statement? v1)))
    (doseq [bad [(assoc s "placement" "unknown") (dissoc s "seed")
                 (assoc s "seed" (identity/seq->bytes [1]))
                 (assoc s "extra" true) (assoc s "v" 1)
                 (assoc s "members" (vec (reverse (get s "members"))))]]
      (is (not (v2/valid-statement? bad))))
    (is (not (identity/bufs= (layout/address v1) (v2/address s))))
    (is (= :rejected (:status (layout/reconcile
                               (layout/make-state (get s "cluster") [(get s "signer")]) s))))))

(deftest seed-is-address-bound-and-epoch-does-not-reshuffle
  (let [s (statement)
        policy {:replicas 3 :write-acks 2 :minimum-domains {"site" 2}}
        successor (assoc s "epoch" 1 "previous" (v2/address s))]
    (is (not (identity/bufs= (v2/address s) (v2/address (assoc s "seed" (bytes32 11))))))
    (is (not (identity/bufs= (v2/address s) (v2/address successor))))
    (doseq [object ["blob-a" "blob-b" "blob-c"]]
      (is (= (v2/place s policy object) (v2/place successor policy object))))))
