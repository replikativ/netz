(ns netz.replication-retirement-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.layout :as layout]
            [netz.replication.retirement :as retirement]
            [netz.replication.wire :as replication-wire]))

(defn await [channel]
  (let [value (<!! channel)]
    (if (instance? Throwable value) (throw value) value)))

(defn signed [identity statement]
  (await (replication-wire/sign-statement identity statement)))

(defn fixture []
  (let [controller (await (identity/generate-identity))
        retired (await (identity/generate-identity))
        replacements (repeatedly 3 #(await (identity/generate-identity)))
        controller-authority (identity/authority-id-bytes (:genesis controller))
        retired-authority (identity/authority-id-bytes (:genesis retired))
        replacement-authorities
        (mapv #(identity/authority-id-bytes (:genesis %)) replacements)
        cluster (identity/seq->bytes (range 16))
        split (blob/split (identity/utf8-bytes "retirement evidence") {:piece-size 8})
        members (fn [authorities]
                  (->> authorities
                       (map-indexed (fn [index authority]
                                      {"authority" authority
                                       "domains" {"site" (str "site-" index)}
                                       "enabled" true}))
                       (sort-by #(identity/bytes->hex (get % "authority"))) vec))
        old-layout {"v" 1 "type" "blob/layout" "cluster" cluster "epoch" 0
                    "placement" "netz/blob-placement/2"
                    "seed" (identity/seq->bytes (repeat 32 9))
                    "previous" (identity/seq->bytes (repeat 32 0))
                    "members" (members (conj (vec (take 2 replacement-authorities))
                                             retired-authority))
                    "issued_at" 900 "signer" controller-authority}
        current-layout (assoc old-layout
                              "epoch" 1 "previous" (layout/address old-layout)
                              "members" (members replacement-authorities)
                              "issued_at" 1000)
        layout-state (-> (layout/make-state cluster [controller-authority])
                         (layout/reconcile old-layout) :state
                         (layout/reconcile current-layout) :state)
        retention {"v" 1 "type" "blob/retention" "cluster" cluster
                   "privacy_domain" (identity/seq->bytes (range 16 32))
                   "revision" 2 "manifest" (:manifest-id split)
                   "object" (:object-id split) "layout_epoch" 1
                   "replicas" 3 "write_acks" 2
                   "placement_minimum_domains" {"site" 2}
                   "ack_minimum_domains" {"site" 2}
                   "issued_at" 1000 "retain_until" 5000
                   "signer" controller-authority}
        retention-envelope (signed controller retention)
        receipts (->> (map (fn [replica authority]
                             (signed replica
                                     {"v" 1 "type" "blob/stored" "cluster" cluster
                                      "privacy_domain" (get retention "privacy_domain")
                                      "retention_revision" 2
                                      "manifest" (:manifest-id split)
                                      "object" (:object-id split) "layout_epoch" 1
                                      "replica" authority "attempt" 1
                                      "persistence" "netz/store/fsync/1"
                                      "stored_at" 1100 "retain_until" 5000
                                      "signer" authority}))
                           replacements replacement-authorities)
                      (sort-by #(identity/bytes->hex
                                 (get-in % ["statement" "replica"]))) vec)
        command {"v" 1 "type" "blob/retirement" "cluster" cluster
                 "privacy_domain" (get retention "privacy_domain")
                 "revision" 3 "manifest" (:manifest-id split)
                 "object" (:object-id split) "layout_epoch" 1
                 "layout" (layout/address current-layout)
                 "previous_layout" (layout/address old-layout)
                 "replica" retired-authority
                 "retention" retention-envelope "receipts" receipts
                 "issued_at" 1150 "retire_after" 1200
                 "signer" controller-authority}
        options {:layout-state layout-state :local-authority retired-authority
                 :controllers [controller-authority]
                 :accepted-persistence #{"netz/store/fsync/1"} :now 1200}]
    {:controller controller :command command :options options
     :target (first replacement-authorities)}))

(deftest all-replacement-receipts-authorize-consideration-only
  (let [{:keys [controller command options target]} (fixture)
        verified (await (retirement/verify-command (signed controller command) options))]
    (is (= "blob/retirement" (get-in verified [:statement "type"])))
    (is (= 3 (count (:targets verified))))
    (is (= 3 (count (:receipts verified))))
    (is (instance? Throwable
                   (<!! (retirement/verify-command
                         (signed controller (update command "receipts" pop)) options))))
    (is (instance? Throwable
                   (<!! (retirement/verify-command
                         (signed controller
                                 (assoc-in command ["receipts" 0 "signature"]
                                           (identity/seq->bytes (repeat 64 0))))
                         options))))
    (is (instance? Throwable
                   (<!! (retirement/verify-command
                         (signed controller (assoc command "replica" target))
                         (assoc options :local-authority target)))))
    (is (instance? Throwable
                   (<!! (retirement/verify-command
                         (signed controller command) (assoc options :now 5000)))))
    (is (instance? Throwable
                   (<!! (retirement/verify-command
                         (signed controller command)
                         (assoc options :accepted-persistence
                                #{"netz/store/volatile/1"})))))
    (is (instance? Throwable
                   (<!! (retirement/verify-command
                         (signed controller command)
                         (assoc-in options [:layout-state :controllers]
                                   #{})))))
    (is (instance? Throwable
                   (<!! (retirement/verify-command
                         (signed controller command)
                         (assoc-in options [:layout-state :conflicts] [{:epoch 1}])))))))
