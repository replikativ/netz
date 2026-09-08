(ns netz.replication-layout-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.layout :as layout]
            [netz.replication.placement :as placement]
            [netz.replication.wire :as replication-wire]
            #?(:clj [clojure.core.async :refer [<!!]]
               :cljs [clojure.core.async :refer [go <!]])
            #?(:cljs [cljs.test :refer [async]])))

(def private-hex
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def controller (identity/authority-id-bytes genesis))
(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))

(def members
  [{"authority" (identity/hex->bytes (apply str (repeat 32 "01")))
    "domains" {"owner" "alice" "site" "van"}
    "enabled" true}
   {"authority" (identity/hex->bytes (apply str (repeat 32 "02")))
    "domains" {"owner" "bob" "site" "fra"}
    "enabled" true}
   {"authority" (identity/hex->bytes (apply str (repeat 32 "03")))
    "domains" {"owner" "carol" "site" "home"}
    "enabled" true}])

(def genesis-layout
  {"v" 1 "type" "blob/layout" "cluster" cluster "epoch" 0
   "placement" "netz/blob-placement/2"
   "seed" (identity/hex->bytes (apply str (repeat 32 "aa")))
   "previous" (identity/hex->bytes (apply str (repeat 32 "00")))
   "members" members "issued_at" 1000
   "signer" controller})

(def layout-address-hex
  "8db12ef36696aded0aa8c1c475c8b952ff0cde8f345e31a47345f172cff1b05e")
(def layout-signature-hex
  "e966fafb9d06fa3310338d77a29d1799b805fab2132214769b2e2923d906bcedc5e2444a5c7bbf3ab04b6f7142d0a4ef5f5440429bb5b4c669850b4da2eb2506")

(defn successor [previous]
  (assoc previous
         "epoch" (inc (get previous "epoch"))
         "previous" (layout/address previous)
         "issued_at" (inc (get previous "issued_at"))))

(deftest signed-layout-schema-and-address-are-portable
  (doseq [invalid [(dissoc genesis-layout "seed")
                   (dissoc genesis-layout "placement")
                   (assoc genesis-layout "placement" "netz/blob-placement/1")
                   (assoc genesis-layout "seed" (apply str (repeat 64 "a")))
                   (assoc genesis-layout "seed" (identity/seq->bytes [1]))]]
    (is (not (layout/valid-statement? invalid))))
  (is (layout/valid-statement? genesis-layout))
  (is (replication-wire/valid-statement? genesis-layout))
  (is (= layout-address-hex
         (identity/bytes->hex (layout/address genesis-layout))))
  (is (= (mapv #(identity/bytes->hex (get % "authority")) members)
         (mapv :id (:members (layout/placement-layout genesis-layout)))))
  (testing "the schema is closed, members are canonical and text is portable"
    (is (not (layout/valid-statement? (assoc genesis-layout "extra" true))))
    (is (not (layout/valid-statement?
              (assoc genesis-layout "members" (vec (reverse members))))))
    (is (not (layout/valid-statement?
              (assoc-in genesis-layout ["members" 0 "domains" "site"]
                        "Montréal"))))))

(deftest reconciliation-accepts-only-an-exact-controller-chain
  (let [initial (layout/make-state cluster [controller])
        accepted (layout/reconcile initial genesis-layout)
        state0 (:state accepted)
        next-layout (successor genesis-layout)
        fork (assoc genesis-layout "issued_at" 1001)
        forked (:state (layout/reconcile state0 fork))]
    (is (= :rejected
           (:status (layout/reconcile
                     state0 (assoc next-layout "seed"
                                   (identity/seq->bytes (repeat 32 1)))))))
    (is (= :accepted (:status accepted)))
    (is (= :duplicate (:status (layout/reconcile state0 genesis-layout))))
    (is (= :conflict (:status (layout/reconcile state0 fork))))
    (is (= 1 (count (:conflicts forked))))
    (is (= 1 (count (:conflicts
                     (:state (layout/reconcile forked fork))))))
    (is (= :forked (:status (layout/reconcile forked next-layout))))
    (is (= :gap (:status (layout/reconcile
                          state0 (successor next-layout)))))
    (is (= :rejected
           (:status (layout/reconcile
                     state0 (assoc next-layout "previous"
                                   (identity/hex->bytes
                                    (apply str (repeat 32 "ff"))))))))
    (let [accepted-next (layout/reconcile state0 next-layout)]
      (is (= :accepted (:status accepted-next)))
      (is (= :stale (:status
                     (layout/reconcile (:state accepted-next)
                                       genesis-layout)))))
    (is (= :rejected
           (:status
            (layout/reconcile
             initial (assoc genesis-layout "signer"
                            (identity/hex->bytes
                             (apply str (repeat 32 "ff"))))))))))

(deftest retention-is-authorized-only-for-current-deterministic-targets
  (let [state (:state (layout/reconcile
                       (layout/make-state cluster [controller])
                       genesis-layout))
        object (identity/hex->bytes (apply str (repeat 32 "a0")))
        retention {"type" "blob/retention" "cluster" cluster
                   "layout_epoch" 0 "object" object
                   "replicas" 2 "write_acks" 2
                   "minimum_domains" {"owner" 2}}
        placement (placement/place
                   (layout/placement-layout genesis-layout)
                   {:replicas 2 :write-acks 2
                    :minimum-domains {"owner" 2}}
                   object)
        selected (identity/hex->bytes (:id (first placement)))
        all (set (map #(identity/bytes->hex (get % "authority")) members))
        unselected (identity/hex->bytes
                    (first (remove (set (map :id placement)) all)))]
    (is (layout/retention-authorized? state selected retention))
    (is (not (layout/retention-authorized? state unselected retention)))
    (is (not (layout/retention-authorized?
              (:state (layout/reconcile state
                                        (assoc genesis-layout
                                               "issued_at" 1001)))
              selected retention)))
    (is (not (layout/retention-authorized?
              state selected (assoc retention "layout_epoch" 1))))))

#?(:clj
   (deftest generic-statement-envelope-signs-layouts
     (let [identity {:genesis genesis
                     :operational {:public (identity/hex->bytes public-hex)
                                   :private (identity/hex->bytes private-hex)}}
           envelope (<!! (replication-wire/sign-statement
                          identity genesis-layout))]
       (is (= layout-signature-hex
              (identity/bytes->hex (get envelope "signature"))))
       (is (true? (<!! (replication-wire/verify-envelope envelope)))))))

#?(:cljs
   (deftest generic-statement-envelope-signs-layouts-cljs
     (async done
            (go
              (let [identity {:genesis genesis
                              :operational
                              {:public (identity/hex->bytes public-hex)
                               :private (identity/hex->bytes private-hex)}}
                    envelope (<! (replication-wire/sign-statement
                                  identity genesis-layout))]
                (is (= layout-signature-hex
                       (identity/bytes->hex (get envelope "signature"))))
                (is (true? (<! (replication-wire/verify-envelope envelope))))
                (done))))))
