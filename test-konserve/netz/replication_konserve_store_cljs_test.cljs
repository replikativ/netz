(ns netz.replication-konserve-store-cljs-test
  (:require [cljs.core.async :as async]
            [cljs.test :refer-macros [async deftest is]]
            [konserve.memory :refer [new-mem-store]]
            [netz.identity :as identity]
            [netz.replication.blob :as blob]
            [netz.replication.konserve-store :as konserve-store]
            [netz.replication.storage :as storage]))

(def session (identity/hex->bytes (apply str (repeat 16 "30"))))
(def split (blob/split (identity/seq->bytes (range 20)) {:piece-size 8}))

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
                                 (:id (second (:pieces split)))))]
          (is (= :ok (:result installed)))
          (is (= "netz/store/volatile/1" (:persistence installed)))
          (is (identity/bufs= (:data (second (:pieces split))) loaded)))
        (done)))))
