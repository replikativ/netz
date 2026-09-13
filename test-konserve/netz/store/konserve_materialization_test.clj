(ns netz.store.konserve-materialization-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is]]
            [konserve.memory :refer [new-mem-store]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.konserve-store :as blob-store]
            [netz.replication.materialization :as object-materialization]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.store.konserve-materialization :as materialization]))

(defn request [bytes]
  {:store-id "dvergr/drive"
   :application-key (identity/utf8-bytes "sha256-hex")
   :codec "bytes/identity"
   :bytes bytes})

(defn await-result [channel] (<!! channel))

(deftest binary-installer-is-idempotent-and-refuses-conflicts
  (let [store (await-result (new-mem-store))
        installer
        (materialization/binary-installer
         store {:store-id "dvergr/drive"
                :decode-key (constantly "sha256-hex")})
        bytes (identity/utf8-bytes "drive payload")]
    (is (= {:result :committed :key "sha256-hex"}
           (await-result (installer (request bytes)))))
    (is (= {:result :already-present :key "sha256-hex"}
           (await-result (installer (request bytes)))))
    (let [conflict
          (await-result
           (installer (request (identity/utf8-bytes "other"))))]
      (is (instance? Throwable conflict))
      (is (= :netz.replication/application-key-conflict
             (:type (ex-data conflict)))))))

(deftest logical-store-and-codec-are-allowlisted-before-writing
  (let [store (await-result (new-mem-store))
        calls (atom 0)
        installer
        (materialization/binary-installer
         store {:store-id "dvergr/drive"
                :decode-key (fn [_] (swap! calls inc) :key)})
        wrong-store (await-result
                     (installer
                      (assoc (request (identity/utf8-bytes "x"))
                             :store-id "geschichte/payload")))
        wrong-codec (await-result
                     (installer
                      (assoc (request (identity/utf8-bytes "x"))
                             :codec "cbor/rfc8949")))]
    (is (= :netz.replication/materialization-store-mismatch
           (:type (ex-data wrong-store))))
    (is (= :netz.replication/unsupported-materialization-codec
           (:type (ex-data wrong-codec))))
    (is (zero? @calls))))

(deftest one-installer-serializes-conflicting-materializations
  (let [store (await-result (new-mem-store))
        installer
        (materialization/binary-installer
         store {:store-id "dvergr/drive" :decode-key (constantly :same-key)})
        first-result (installer (request (identity/utf8-bytes "first")))
        second-result (installer (request (identity/utf8-bytes "second")))
        results [(await-result first-result) (await-result second-result)]
        committed (filter #(= :committed (:result %)) results)
        conflicts (filter #(and (instance? Throwable %)
                                (= :netz.replication/application-key-conflict
                                   (:type (ex-data %))))
                          results)]
    (is (= 1 (count committed)))
    (is (= 1 (count conflicts)))))

(deftest installed-netz-object-materializes-into-an-ordinary-konserve-key
  (let [underlying (await-result (new-mem-store))
        repository (blob-store/new-konserve-blob-store
                    underlying {:persistence "netz/store/volatile/1"})
        value (identity/utf8-bytes "room drive attachment")
        split (blob/split value {:piece-size 6})
        session (identity/seq->bytes (range 16))
        public-hex
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
        genesis
        (identity/make-genesis
         {:rotation-keys [(apply str (repeat 32 "aa"))]
          :rotation-threshold 1
          :operational-keys [public-hex]
          :revocation-commit (apply str (repeat 32 "cc"))})
        authority (identity/authority-id-bytes genesis)
        retention
        {"v" 1 "type" "blob/retention"
         "cluster" (identity/seq->bytes (range 16))
         "privacy_domain" (identity/seq->bytes (range 16 32))
         "revision" 1 "manifest" (:manifest-id split)
         "object" (:object-id split) "layout_epoch" 1
         "replicas" 1 "write_acks" 1
         "placement_minimum_domains" {} "ack_minimum_domains" {}
         "issued_at" 1000 "retain_until" 5000 "signer" authority}
        retention-envelope
        (replication-wire/envelope
         genesis (identity/hex->bytes public-hex) retention
         (identity/seq->bytes (repeat 64 0)))
        app-binding
        (object-materialization/application-binding
         {:store-id "dvergr/drive"
          :application-key (identity/utf8-bytes "sha256-hex")
          :codec "bytes/identity"
          :object-id (:object-id split)
          :manifest-id (:manifest-id split)})
        installer
        (materialization/binary-installer
         underlying {:store-id "dvergr/drive"
                     :decode-key (constantly "sha256-hex")})]
    (doseq [{:keys [index id data]} (:pieces split)]
      (is (= :ok
             (await-result
              (storage/-stage-piece!
               repository session (:manifest-id split) (:manifest split)
               index id data)))))
    (is (= :ok
           (:result
            (await-result
             (storage/-install-object!
              repository session (:manifest-id split) (:manifest split)
              retention "netz/store/volatile/1")))))
    (let [result
          (await-result
           (object-materialization/materialize!
            repository nil [] retention-envelope app-binding installer))]
      (is (= :materialized (:status result)))
      (is (= :local (:source result)))
      (is (= :committed (get-in result [:install-result :result]))))
    (is (= :already-present
           (:result (await-result (installer (request value))))))))
