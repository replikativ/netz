(ns netz.replication-konserve-store-cljs-test
  (:require [cljs.core.async :as async]
            [cljs.test :refer-macros [async deftest is]]
            [konserve.memory :refer [new-mem-store]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.inventory :as inventory]
            [netz.replication.konserve-store :as konserve-store]
            [netz.replication.lifecycle :as lifecycle]
            [netz.replication.lifecycle-runtime :as lifecycle-runtime]
            [netz.replication.storage :as storage]
            [netz.wire :as wire]))

(def session (identity/hex->bytes (apply str (repeat 16 "30"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))
(def public-hex
  "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a")
(def genesis
  (identity/make-genesis
   {:rotation-keys [(apply str (repeat 32 "aa"))]
    :rotation-threshold 1
    :operational-keys [public-hex]
    :revocation-commit (apply str (repeat 32 "cc"))}))
(def authority (identity/authority-id-bytes genesis))
(def retention
  {"v" 1 "type" "blob/retention"
   "cluster" (identity/hex->bytes (apply str (repeat 16 "10")))
   "privacy_domain" (identity/hex->bytes (apply str (repeat 16 "20")))
   "revision" 1 "manifest" (:manifest-id split)
   "object" (:object-id split) "layout_epoch" 7
   "replicas" 1 "write_acks" 1 "minimum_domains" {}
   "issued_at" 1000 "retain_until" 5000 "signer" authority})

(deftest portable-konserve-store-resumes-and-installs
  (async done
    (async/go
      (let [underlying (async/<! (new-mem-store))
            make-store #(konserve-store/new-konserve-blob-store
                         underlying {:persistence "netz/store/volatile/1"
                                     :clock (constantly 1200)})
            first-run (make-store)
            piece-0 (first (:pieces split))
            staged (async/<! (storage/-stage-piece!
                              first-run session
                              (:manifest-id split) (:manifest split)
                              (:index piece-0) (:id piece-0) (:data piece-0)))
            restarted (make-store)
            inventory (async/<! (storage/-load-staging
                                 restarted session
                                 (:manifest-id split) (:manifest split)))]
        (is (= :ok staged))
        (is (= 1 (count (:held inventory))))
        (is (instance?
             js/Error
             (async/<! (storage/-stage-piece!
                        restarted session
                        (:manifest-id split) (:manifest split)
                        1 (:id (second (:pieces split)))
                        (identity/seq->bytes [0]))))
            "invalid storage input is delivered as a failure value")
        (doseq [piece (rest (:pieces split))]
          (is (= :ok
                 (async/<! (storage/-stage-piece!
                            restarted session
                            (:manifest-id split) (:manifest split)
                            (:index piece) (:id piece) (:data piece))))))
        (let [installed (async/<! (storage/-install-object!
                                   restarted session
                                   (:manifest-id split) (:manifest split)
                                   {} "netz/store/volatile/1"))
              loaded (async/<! (storage/-load-piece
                                 restarted session (:manifest-id split) 1
                                 (:id (second (:pieces split)))))
              page (async/<! (inventory/inventory-page restarted nil 10))
              manifest (async/<! (inventory/installed-manifest
                                  restarted (:manifest-id split)))
              health (async/<! (inventory/scrub-object!
                                 restarted (:manifest-id split)))]
          (is (= :ok (:result installed)))
          (is (= "netz/store/volatile/1" (:persistence installed)))
          (is (identity/bufs= (:data (second (:pieces split))) loaded))
          (is (= 1 (count (:manifests page))))
          (is (identity/bufs= (wire/encode-canonical (:manifest split))
                              (wire/encode-canonical manifest)))
          (is (= :healthy (:status health))))
        (done)))))

(deftest portable-lifecycle-policy-restarts-from-konserve
  (async done
    (async/go
      (let [underlying (async/<! (new-mem-store))
            make-store #(konserve-store/new-konserve-blob-store
                         underlying {:persistence "netz/store/volatile/1"})
            make-runtime
            #(lifecycle-runtime/start!
              {:identity {:genesis genesis}
               :store (make-store)
               :controllers [authority]
               :lifecycle-opts
               {:persistence "netz/store/volatile/1"
                :accepted-persistence #{"netz/store/volatile/1"}}
               :now-fn (constantly 1000)})
            request {:from :controller :from-authority authority
                     :retention retention :frame {"replica" authority}}
            first-run (make-runtime)]
        (is (true? (async/<! (lifecycle-runtime/ready first-run))))
        (is (true? (async/<! (lifecycle-runtime/authorize-offer!
                              first-run request))))
        (lifecycle-runtime/stop! first-run)
        (let [restarted (make-runtime)]
          (is (true? (async/<! (lifecycle-runtime/ready restarted))))
          (is (= :retained
                 (lifecycle/status (lifecycle-runtime/state restarted)
                                   retention)))
          (lifecycle-runtime/stop! restarted))
        (done)))))
