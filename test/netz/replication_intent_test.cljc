(ns netz.replication-intent-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.intent :as intent]
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
(def signer
  {:genesis genesis
   :operational {:public (identity/hex->bytes public-hex)
                 :private (identity/hex->bytes private-hex)}})
(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))
(def privacy-domain (identity/hex->bytes (apply str (repeat 16 "20"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))
(def members
  (mapv (fn [pair owner]
          {"authority" (identity/hex->bytes (apply str (repeat 32 pair)))
           "domains" {"owner" owner}
           "enabled" true})
        ["01" "02" "03"] ["alice" "bob" "carol"]))
(def layout-statement
  {"v" 1 "type" "blob/layout" "cluster" cluster "epoch" 0
   "placement" "netz/blob-placement/2"
   "seed" (identity/hex->bytes (apply str (repeat 32 "aa")))
   "previous" (identity/hex->bytes (apply str (repeat 32 "00")))
   "members" members "issued_at" 900 "signer" controller})
(def retention
  {"v" 1 "type" "blob/retention" "cluster" cluster
   "privacy_domain" privacy-domain "revision" 1
   "manifest" (:manifest-id split) "object" (:object-id split)
   "layout_epoch" 0 "replicas" 2 "write_acks" 2
   "minimum_domains" {"owner" 2} "issued_at" 1000
   "retain_until" 5000 "signer" controller})

(defn fake-envelope [statement]
  (replication-wire/envelope
   genesis (identity/hex->bytes public-hex) statement
   (identity/seq->bytes (repeat identity/signature-size 0))))

(def fake-layout (fake-envelope layout-statement))
(def fake-retention (fake-envelope retention))
(def targets
  (mapv :id
        (placement/place (layout/placement-layout layout-statement)
                         {:replicas 2 :write-acks 2
                          :minimum-domains {"owner" 2}}
                         (:object-id split))))
(def attempts (zipmap targets [3 5]))

(deftest durable-intent-record-is-closed-and-deterministic
  (let [record (intent/make-record fake-layout fake-retention
                                   (:manifest split) attempts 7)
        restored (intent/record->intent record)]
    (is (intent/valid-record? record))
    (is (= (sort targets)
           (mapv #(identity/bytes->hex (get % "replica"))
                 (get record "attempts"))))
    (is (= attempts (:attempts restored)))
    (is (= 7 (:intent-revision restored)))
    (is (= (set (map #(identity/bytes->hex (get % "authority")) members))
           (set (map :id (get-in restored [:layout :members])))))
    (is (= (:manifest split) (get-in restored [:manifest :wire-manifest])))
    (is (= (identity/bytes->hex (:object-id split))
           (get-in restored [:manifest :object-id])))
    (is (intent/same-record? record
                             (intent/make-record fake-layout fake-retention
                                                 (:manifest split)
                                                 (into {} (reverse attempts)) 7)))
    (testing "closed fields, canonical attempts and derived targets fail closed"
      (is (not (intent/valid-record? (assoc record "extra" true))))
      (is (not (intent/valid-record?
                (update record "attempts" #(vec (reverse %))))))
      (is (not (intent/valid-record?
                (assoc-in record ["attempts" 0 "attempt"] -1))))
      (is (not (intent/valid-record?
                (update record "attempts" pop))))
      (is (not (intent/valid-record?
                (assoc-in record ["manifest" "length"] 21)))))))

(deftest successor-record-renews-policy-and-fences-every-target
  (let [current (intent/make-record fake-layout fake-retention
                                    (:manifest split) attempts 7)
        renewal (assoc retention "revision" 2 "issued_at" 1100
                       "retain_until" 6000 "replicas" 3)
        successor (intent/make-successor-record
                   current fake-layout (fake-envelope renewal))
        next-attempts (:attempts (intent/record->intent successor))
        added-target (first (remove (set targets) (keys next-attempts)))]
    (is (intent/valid-successor-record? current successor))
    (is (= 8 (get successor "intent_revision")))
    (is (= (inc (get attempts (first targets)))
           (get next-attempts (first targets))))
    (is (= 8 (get next-attempts added-target)))
    (is (not (intent/valid-successor-record?
              current
              (intent/make-record
               fake-layout
               (fake-envelope (assoc renewal "retain_until" 4000))
               (:manifest split) next-attempts 8))))))

#?(:clj
   (deftest signatures-and-controller-authorization-are-reverified
     (let [layout-envelope (<!! (replication-wire/sign-statement
                                 signer layout-statement))
           retention-envelope (<!! (replication-wire/sign-statement
                                    signer retention))
           record (intent/make-record layout-envelope retention-envelope
                                      (:manifest split) attempts 0)
           verified (<!! (intent/verify-record record [controller]))]
       (is (map? verified))
       (is (= attempts (:attempts verified)))
       (is (instance? Throwable
                      (<!! (intent/verify-record
                            record [(identity/hex->bytes
                                     (apply str (repeat 32 "ff")))]))))
       (is (instance? Throwable
                      (<!! (intent/verify-record
                            (update-in record ["retention" "signature"]
                                       #(identity/seq->bytes
                                         (cons 1 (rest %))))
                            [controller])))))))

#?(:cljs
   (deftest signatures-and-controller-authorization-are-reverified-cljs
     (async done
            (go
              (let [layout-envelope (<! (replication-wire/sign-statement
                                         signer layout-statement))
                    retention-envelope (<! (replication-wire/sign-statement
                                            signer retention))
                    record (intent/make-record layout-envelope retention-envelope
                                               (:manifest split) attempts 0)
                    verified (<! (intent/verify-record record [controller]))]
                (is (map? verified))
                (is (= attempts (:attempts verified)))
                (is (instance? js/Error
                               (<! (intent/verify-record
                                    record [(identity/hex->bytes
                                             (apply str (repeat 32 "ff")))]))))
                (done))))))
