(ns netz.replication-konserve-store-test
  (:refer-clojure :exclude [await])
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [konserve.core :as k]
            [konserve.memory :refer [new-mem-store]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.inventory :as inventory]
            [netz.replication.konserve-store :as konserve-store]
            [netz.replication.layout :as layout]
            [netz.replication.layout-runtime :as layout-runtime]
            [netz.replication.lifecycle :as lifecycle]
            [netz.replication.lifecycle-runtime :as lifecycle-runtime]
            [netz.replication.lifecycle-storage :as lifecycle-storage]
            [netz.replication.storage :as storage]
            [netz.replication.wire :as replication-wire]
            [netz.wire :as wire]))

(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def private-hex
  "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def authority (identity/authority-id-bytes genesis))
(def session (identity/hex->bytes (apply str (repeat 16 "30"))))
(def session-2 (identity/hex->bytes (apply str (repeat 16 "31"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))

(defn await [channel]
  (let [result (<!! channel)]
    (if (instance? Throwable result) (throw result) result)))

(defn fresh-store
  ([] (fresh-store (<!! (new-mem-store))))
  ([underlying]
   (konserve-store/new-konserve-blob-store
    underlying {:persistence "netz/store/volatile/1"
                :clock (constantly 1200)})))

(defn stage! [store piece]
  (await (storage/-stage-piece!
          store session (:manifest-id split) (:manifest split)
          (:index piece) (:id piece) (:data piece))))

(defn install! [store]
  (await (storage/-install-object!
          store session (:manifest-id split) (:manifest split)
          {} "netz/store/volatile/1")))

(defn sign-statement [statement]
  (await (replication-wire/sign-statement
          {:genesis genesis
           :operational {:public (identity/hex->bytes public-hex)
                         :private (identity/hex->bytes private-hex)}}
          statement)))

(defn signed-receipt []
  (sign-statement
   {"v" 1
    "type" "blob/stored"
    "cluster" (identity/hex->bytes (apply str (repeat 16 "10")))
    "privacy_domain" (identity/hex->bytes (apply str (repeat 16 "20")))
    "retention_revision" 1
    "manifest" (:manifest-id split)
    "object" (:object-id split)
    "layout_epoch" 7
    "replica" authority
    "attempt" 0
    "persistence" "netz/store/volatile/1"
    "stored_at" 1200
    "retain_until" 5000
    "signer" authority}))

(defn controller-event [receipt]
  {:type :replication/stored
   :object-id (identity/bytes->hex (:object-id split))
   :replica-id (identity/bytes->hex authority)
   :layout-epoch 7
   :attempt 0
   :persistence "netz/store/volatile/1"
   :receipt receipt
   :manifest-id (identity/bytes->hex (:manifest-id split))})

(def retention
  {"v" 1 "type" "blob/retention"
   "cluster" (identity/hex->bytes (apply str (repeat 16 "10")))
   "privacy_domain" (identity/hex->bytes (apply str (repeat 16 "20")))
   "revision" 1 "manifest" (:manifest-id split)
   "object" (:object-id split) "layout_epoch" 7
   "replicas" 1 "write_acks" 1 "minimum_domains" {}
   "issued_at" 1000 "retain_until" 5000 "signer" authority})

(def layout0
  {"v" 1 "type" "blob/layout"
   "cluster" (get retention "cluster") "epoch" 0
   "previous" (identity/hex->bytes (apply str (repeat 32 "00")))
   "members" [{"authority" authority "domains" {} "enabled" true}]
   "issued_at" 900 "signer" authority})

(deftest pieces-resume-and-install-through-konserve
  (let [underlying (<!! (new-mem-store))
        first-run (fresh-store underlying)
        piece-0 (first (:pieces split))]
    (is (= {:held [] :installed nil}
           (await (storage/-load-staging
                   first-run session (:manifest-id split) (:manifest split)))))
    (is (= :ok (stage! first-run piece-0)))
    (is (true? (await (storage/-cleanup-session!
                       first-run session (:manifest-id split)))))

    (let [restarted (fresh-store underlying)
          inventory (await (storage/-load-staging
                            restarted session-2 (:manifest-id split)
                            (:manifest split)))]
      (is (= 1 (count (:held inventory))))
      (is (identity/bufs= (:id piece-0) (first (:held inventory))))
      (is (nil? (:installed inventory)))

      (doseq [piece (rest (:pieces split))] (stage! restarted piece))
      (let [installed (install! restarted)]
        (is (= :ok (:result installed)))
        (is (= "netz/store/volatile/1" (:persistence installed)))
        (is (= 1200 (:stored-at installed))))

      (let [loaded (await (storage/-load-piece
                           restarted session-2 (:manifest-id split) 1
                           (:id (second (:pieces split)))))]
        (is (identity/bufs= (:data (second (:pieces split))) loaded)))

      (is (= "netz/store/volatile/1"
             (get-in (await (storage/-load-staging
                             restarted session-2 (:manifest-id split)
                             (:manifest split)))
                     [:installed :persistence]))))))

(deftest invalid-content-and-persistence-fail-closed
  (let [store (fresh-store)
        piece (first (:pieces split))]
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"invalid piece"
         (await (storage/-stage-piece!
                 store session (:manifest-id split) (:manifest split)
                 0 (:id piece) (identity/seq->bytes [0])))))
    (doseq [value (butlast (:pieces split))] (stage! store value))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"missing or corrupt"
                          (install! store)))
    (stage! store (last (:pieces split)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"does not match"
         (await (storage/-install-object!
                 store session (:manifest-id split) (:manifest split)
                 {} "netz/store/best-effort/1"))))))

(deftest signed-receipts-survive-wrapper-restart
  (let [underlying (<!! (new-mem-store))
        store (fresh-store underlying)
        receipt (signed-receipt)
        event (controller-event receipt)]
    (is (true? (await (replication-wire/verify-envelope receipt))))
    (is (= :ok (await (storage/-persist-receipt! store session event))))

    (let [replayed (first
                    (await (storage/-recorded-receipts
                            (fresh-store underlying))))]
      (is (= (dissoc event :receipt) (dissoc replayed :receipt)))
      (is (identity/bufs= (wire/encode-canonical receipt)
                          (wire/encode-canonical (:receipt replayed)))))

    (testing "receipt replay is bounded"
      (is (= :ok (await (storage/-persist-receipt!
                         store session-2 event))))
      (let [bounded (konserve-store/new-konserve-blob-store
                     underlying {:persistence "netz/store/volatile/1"
                                 :max-receipts 1})]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"configured bound"
                              (await (storage/-recorded-receipts bounded))))))))

(deftest namespaces-and-durability-claims-are-explicit
  (let [underlying (<!! (new-mem-store))
        a (konserve-store/new-konserve-blob-store
           underlying {:prefix :a :persistence "netz/store/volatile/1"})
        b (konserve-store/new-konserve-blob-store
           underlying {:prefix :b :persistence "netz/store/volatile/1"})]
    (stage! a (first (:pieces split)))
    (is (= 1 (count (:held
                     (await (storage/-load-staging
                             a session (:manifest-id split) (:manifest split)))))))
    (is (empty? (:held
                 (await (storage/-load-staging
                         b session (:manifest-id split) (:manifest split))))))

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"Invalid Konserve blob-store options"
         (konserve-store/new-konserve-blob-store
          underlying {:persistence "netz/store/fsync/1"})))

    ;; The repository uses ordinary Konserve top-level keys and does not alter
    ;; unrelated application data in the shared store.
    (await (k/assoc underlying :application/value 42 {:sync? false}))
    (is (= 42 (await (k/get underlying :application/value nil
                            {:sync? false}))))))

(deftest lifecycle-policy-journal-restarts-through-the-same-konserve
  (let [underlying (<!! (new-mem-store))
        replica-identity {:genesis genesis
                          :operational
                          {:public (identity/hex->bytes public-hex)
                           :private (identity/hex->bytes private-hex)}}
        make-store #(fresh-store underlying)
        make-runtime
        #(lifecycle-runtime/start!
          {:identity replica-identity
           :store (make-store)
           :controllers [authority]
           :lifecycle-opts
           {:persistence "netz/store/volatile/1"
            :accepted-persistence #{"netz/store/volatile/1"}}
           :now-fn (constantly 1000)})
        request {:from :controller :from-authority authority
                 :retention retention :frame {"replica" authority}}
        first-run (make-runtime)]
    (is (true? (await (lifecycle-runtime/ready first-run))))
    (is (true? (await (lifecycle-runtime/authorize-offer!
                       first-run request))))
    (lifecycle-runtime/stop! first-run)

    (let [restarted (make-runtime)]
      (is (true? (await (lifecycle-runtime/ready restarted))))
      (is (= :retained
             (lifecycle/status (lifecycle-runtime/state restarted)
                               retention)))
      (is (instance?
           Throwable
           (<!! (lifecycle-storage/-delete-object!
                 (:store restarted) "object-key" retention
                 "netz/store/volatile/1")))
          "generic Konserve refuses to claim marker-only privacy erasure")
      (lifecycle-runtime/stop! restarted))))

(deftest signed-layout-transcript-restarts-through-the-same-konserve
  (let [underlying (<!! (new-mem-store))
        make-runtime #(layout-runtime/start!
                       {:store (fresh-store underlying)
                        :cluster (get layout0 "cluster")
                        :controllers [authority]})
        envelope (sign-statement layout0)
        first-run (make-runtime)]
    (is (true? (await (layout-runtime/ready first-run))))
    (is (= :accepted
           (await (layout-runtime/apply-envelope! first-run envelope))))
    (layout-runtime/stop! first-run)
    (let [restarted (make-runtime)]
      (is (true? (await (layout-runtime/ready restarted))))
      (is (= 0 (get-in (layout-runtime/state restarted)
                       [:current :statement "epoch"])))
      (is (identity/bufs= (layout/address layout0)
                          (get-in (layout-runtime/state restarted)
                                  [:current :address])))
      (layout-runtime/stop! restarted))))

(deftest installed-konserve-objects-are-inventoried-and-scrubbed
  (let [underlying (<!! (new-mem-store))
        store (fresh-store underlying)]
    (doseq [piece (:pieces split)] (stage! store piece))
    (install! store)
    (let [page (await (inventory/inventory-page store nil 10))
          manifest (await (inventory/installed-manifest
                           store (:manifest-id split)))
          healthy (await (inventory/scrub-object! store (:manifest-id split)))]
      (is (= 1 (count (:manifests page))))
      (is (identity/bufs= (:manifest-id split) (first (:manifests page))))
      (is (nil? (:next-cursor page)))
      (is (identity/bufs= (wire/encode-canonical (:manifest split))
                          (wire/encode-canonical manifest)))
      (is (= :healthy (:status healthy)))
      (is (identity/bufs= (:object-id split) (:object healthy))))
    (is (nil? (await (inventory/installed-manifest
                      store (identity/sha256
                             (identity/utf8-bytes "absent"))))))
    (is (= :missing
           (:status (await (inventory/scrub-object!
                            store (identity/sha256
                                   (identity/utf8-bytes "absent")))))))
    (let [piece (first (:pieces split))
          key [:netz.blob :netz.replication.konserve-store/piece
               (identity/bytes->hex (:id piece))]]
      (await (k/assoc underlying key (identity/seq->bytes (repeat 8 0))
                      {:sync? false}))
      (is (instance? Throwable
                     (<!! (inventory/scrub-object!
                           store (:manifest-id split))))))))
