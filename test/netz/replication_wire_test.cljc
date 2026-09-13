(ns netz.replication-wire-test
  (:require [clojure.test :refer [deftest is testing]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.retirement :as retirement]
            [netz.replication.wire :as replication-wire]
            #?(:clj [clojure.core.async :refer [<!!]])))

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

(def cluster (identity/hex->bytes (apply str (repeat 16 "10"))))
(def privacy-domain (identity/hex->bytes (apply str (repeat 16 "20"))))
(def session (identity/hex->bytes (apply str (repeat 16 "30"))))
(def signer (identity/authority-id-bytes genesis))

(def signing-hex
  "6e65747a2f626c6f622d73746174656d656e742f763100af61760164747970656e626c6f622f726574656e74696f6e666f626a6563745820c959701fbfcec8aeb4795de542de04c74d60e2736ce017c39f148c50604f53e0667369676e65725820dadc6166b57499e9a4934c894f012a4acd29d851521f5be9dc04def7937467ca67636c75737465725010101010101010101010101010101010686d616e69666573745820b40648f31978841528e7a438f42b99de987fa22c75f2f1dca334c358dd5da23c687265706c6963617303687265766973696f6e01696973737565645f61741903e86a77726974655f61636b73026c6c61796f75745f65706f6368076c72657461696e5f756e74696c1907d06e707269766163795f646f6d61696e50202020202020202020202020202020207361636b5f6d696e696d756d5f646f6d61696e73a2647369746502656f776e6572027819706c6163656d656e745f6d696e696d756d5f646f6d61696e73a2647369746502656f776e657202")
(def signature-hex
  "050fcd85883d68dd6f803b84d6bd2c6f3ceff14672369dec65d26913d9319143c29a8e18dace44e5a531d978badfd0595de7a4103f3fa6fbd1c76e9b2603e107")
(def offer-hash-hex
  "d010d089c94a52fa8b903e3389114d731655ae0116392ea57efeb1b18a338a0e")
(def fetch-hash-hex
  "01d4015c0d9bcddcb71e144059ea099a46015680e9289c59667ef030469dcb06")
(def need-hash-hex
  "3f796a327746eb456ff78368e5924220777d5de74e71e5d625c27a3799d25709")
(def piece-hash-hexes
  ["1e51bd8f1d4097b970e746ffef7dca9729ac8c4d5d83ad93e1e3d5804863a808"
   "3db39ba3dd8c59f30a9349b75860e3295a1a74e6cef1fde760c4a254b5687c99"
   "64f23eee5c9b25ce1a9506eb74b5bd8692e024b04783e0923a405ca6ac247226"])
(def ack-hash-hexes
  ["889f03fee2c9b4a037d8c6579a7a20e3db7c24c3e75bad2371456b0116921f3f"
   "a0b0454cc4380105aa9e83bd1d8adf781d221f0d306e1a979d03475c7421baf6"
   "82a0bc0b98c22a8fb5c4b06085dde2e1571669222e9ed9c48a720e19e6179bbb"])
(def commit-hash-hex
  "d6796cacf36b17019fe24486aced3ec2c3f5a61175ac1bfedacab8bf3a939915")

(def split
  (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))

(def retention
  {"v" 1
   "type" "blob/retention"
   "cluster" cluster
   "privacy_domain" privacy-domain
   "revision" 1
   "manifest" (:manifest-id split)
   "object" (:object-id split)
   "layout_epoch" 7
   "replicas" 3
   "write_acks" 2
   "placement_minimum_domains" {"owner" 2 "site" 2}
   "ack_minimum_domains" {"owner" 2 "site" 2}
   "issued_at" 1000
   "retain_until" 2000
   "signer" signer})

(defn fake-envelope [statement]
  (replication-wire/envelope
   genesis (identity/hex->bytes public-hex) statement
   (identity/hex->bytes signature-hex)))

(deftest signed-statements-are-closed-and-domain-separated
  (is (replication-wire/valid-statement? retention))
  (is (= signing-hex
         (identity/bytes->hex
          (replication-wire/signing-bytes retention))))
  (is (not (replication-wire/valid-statement?
            (assoc retention "extension" true))))
  (is (not (replication-wire/valid-statement?
            (assoc retention "write_acks" 4))))
  (is (not= (identity/bytes->hex
             (replication-wire/signing-bytes retention))
            (identity/bytes->hex
             (replication-wire/signing-bytes
              (update retention "retain_until" inc))))))

(deftest replica-receipts-bind-durability-and-attempt
  (let [receipt {"v" 1
                 "type" "blob/stored"
                 "cluster" cluster
                 "privacy_domain" privacy-domain
                 "retention_revision" 1
                 "manifest" (:manifest-id split)
                 "object" (:object-id split)
                 "layout_epoch" 7
                 "replica" signer
                 "attempt" 3
                 "persistence" "netz/store/fsync/1"
                 "stored_at" 1200
                 "retain_until" 2000
                 "signer" signer}]
    (is (replication-wire/valid-statement? receipt))
    (is (not (replication-wire/valid-statement?
              (assoc receipt "persistence" "durable-ish"))))
    (is (not (replication-wire/valid-statement?
              (assoc receipt "signer"
                     (identity/hex->bytes (apply str (repeat 32 "ff")))))))
    (is (replication-wire/valid-frame?
         {"v" 1 "type" "blob/stored" "session" session
          "receipt" (fake-envelope receipt)}))))

(deftest transfer-frames-are-bounded-and-bind-the-manifest
  (let [offer {"v" 1
               "type" "blob/offer"
               "session" session
               "attempt" 0
               "replica" signer
               "retention" (fake-envelope retention)
               "manifest" (:manifest split)}
        encoded (replication-wire/encode-frame offer)]
    (is (replication-wire/valid-frame? offer))
    (is (replication-wire/valid-frame?
         (replication-wire/decode-frame encoded)))
    (is (= offer-hash-hex
           (identity/bytes->hex (identity/sha256 encoded))))
    (is (not (replication-wire/valid-frame?
              (assoc-in offer ["manifest" "object"]
                        (identity/hex->bytes (apply str (repeat 32 "ff")))))))
    (is (replication-wire/valid-frame?
         {"v" 1 "type" "blob/need" "session" session
          "manifest" (:manifest-id split)
          "need" (blob/need-bitmap (:manifest split) #{})}))
    (is (replication-wire/valid-frame?
         {"v" 1 "type" "blob/piece" "session" session
          "manifest" (:manifest-id split) "index" 0
          "data" (:data (first (:pieces split)))}))
    (is (not (replication-wire/valid-frame?
              {"v" 1 "type" "blob/abort" "session" session
               "code" ""})))))

(deftest transfer-trace-bytes-match-python
  (let [hash-frame #(-> % replication-wire/encode-frame identity/sha256
                        identity/bytes->hex)
        need {"v" 1 "type" "blob/need" "session" session
              "manifest" (:manifest-id split)
              "need" (blob/need-bitmap (:manifest split) #{})}
        piece-frames
        (mapv (fn [{:keys [index data]}]
                {"v" 1 "type" "blob/piece" "session" session
                 "manifest" (:manifest-id split) "index" index "data" data})
              (:pieces split))
        ack-frames
        (mapv (fn [{:keys [index]}]
                {"v" 1 "type" "blob/ack" "session" session
                 "manifest" (:manifest-id split) "through" index})
              (:pieces split))
        commit {"v" 1 "type" "blob/commit" "session" session
                "manifest" (:manifest-id split)
                "count" (count (:pieces split))}]
    (is (= need-hash-hex (hash-frame need)))
    (is (= piece-hash-hexes (mapv hash-frame piece-frames)))
    (is (= ack-hash-hexes (mapv hash-frame ack-frames)))
    (is (= commit-hash-hex (hash-frame commit)))))

(deftest fetch-frames-bind-the-signed-retention-request
  (let [fetch {"v" 1 "type" "blob/fetch" "session" session
               "attempt" 0 "retention" (fake-envelope retention)}
        encoded (replication-wire/encode-frame fetch)]
    (is (replication-wire/valid-frame? fetch))
    (is (= fetch-hash-hex
           (identity/bytes->hex (identity/sha256 encoded))))
    (is (replication-wire/valid-frame?
         (replication-wire/decode-frame encoded)))
    (is (not (replication-wire/valid-frame?
              (assoc fetch "attempt" -1))))
    (is (not (replication-wire/valid-frame?
              (assoc-in fetch ["retention" "statement" "type"]
                        "blob/lease"))))))

(deftest lease-and-deletion-have-independent-monotone-revisions
  (let [common {"v" 1 "cluster" cluster "privacy_domain" privacy-domain
                "manifest" (:manifest-id split) "object" (:object-id split)
                "layout_epoch" 7 "issued_at" 1500 "signer" signer}
        lease (assoc common "type" "blob/lease" "revision" 2
                     "retain_until" 3000)
        deletion (assoc common "type" "blob/deletion" "revision" 3
                        "delete_after" 1600)
        deleted {"v" 1 "type" "blob/deleted" "cluster" cluster
                 "privacy_domain" privacy-domain "deletion_revision" 3
                 "manifest" (:manifest-id split) "object" (:object-id split)
                 "layout_epoch" 7 "replica" signer
                 "persistence" "netz/store/fsync/1" "deleted_at" 1700
                 "signer" signer}]
    (is (replication-wire/valid-statement? lease))
    (is (replication-wire/valid-statement? deletion))
    (is (replication-wire/valid-statement? deleted))))

(deftest retirement-verifier-is-portable
  ;; Requiring this namespace from the .cljc suite keeps its complete
  ;; dependency graph in both the JVM and Node compilation gates.
  (is (some? retirement/verify-command)))

#?(:clj
   (deftest statement-signatures-verify-and-tampering-fails
     (let [identity {:genesis genesis
                     :operational {:public (identity/hex->bytes public-hex)
                                   :private (identity/hex->bytes private-hex)}}
           signed (<!! (replication-wire/sign-statement identity retention))]
       (is (= signature-hex
              (identity/bytes->hex (get signed "signature"))))
       (is (true? (<!! (replication-wire/verify-envelope signed))))
       (is (false? (<!! (replication-wire/verify-envelope
                         (update-in signed ["statement" "retain_until"] inc)))))
       (testing "another Authority cannot be substituted around a valid signature"
         (is (thrown? Throwable
                      (replication-wire/envelope
                       genesis (identity/hex->bytes public-hex)
                       (assoc retention "signer"
                              (identity/hex->bytes
                               (apply str (repeat 32 "ff"))))
                       (get signed "signature"))))))))
