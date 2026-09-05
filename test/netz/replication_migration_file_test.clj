(ns netz.replication-migration-file-test
  (:require [clojure.test :refer [deftest is]]
            [netz.replication.migration :as migration]
            [netz.replication.migration-file :as file]
            [netz.replication-migration-test :as model])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]
           [java.security SecureRandom]))

(deftest authenticated-checkpoint-reopen-and-rejection
  (let [dir (Files/createTempDirectory "netz-migration-test-" (make-array FileAttribute 0))
        path (.resolve dir "checkpoint")
        key (byte-array 32)
        _ (.nextBytes (SecureRandom.) key)
        template (model/initial)]
    (try
      (is (thrown? java.nio.file.NoSuchFileException (file/load! path template key 0)))
      (let [saved (file/persist! path template key template)
            original (Files/readAllBytes path)
            reopened (file/load! path template key 0)
            replica (first (keys (:attempts reopened)))
            pending (:state (migration/step reopened (model/receipt reopened replica)))]
        (is (migration/ready? saved))
        (is (= saved reopened))
        (is (empty? (model/candidates reopened)))
        (is (= pending (assoc (file/persist! path template key pending) :persisted 0)))
        (is (= 1 (:revision (file/load! path template key 1))))
        (is (thrown? Exception (file/load! path template (byte-array 32) 0)))
        (is (thrown? Exception (file/load! path (assoc-in template [:binding :object] "other") key 0)))
        (is (thrown? Exception (file/validate-record template
                                                     (assoc (migration/checkpoint pending) :targets []))))
        ;; An authentic old file is rejected only with an independent revision floor.
        (Files/write path original (make-array java.nio.file.OpenOption 0))
        (is (thrown? Exception (file/load! path template key 1)))
        (aset-byte original 40 (unchecked-byte (bit-xor 1 (aget original 40))))
        (Files/write path original (make-array java.nio.file.OpenOption 0))
        (is (thrown? Exception (file/load! path template key 0)))
        (Files/write path (byte-array 12) (make-array java.nio.file.OpenOption 0))
        (is (thrown? Exception (file/load! path template key 0))))
      (finally
        (Files/deleteIfExists path)
        (Files/deleteIfExists dir)))))

(deftest io-boundary-failures-never-acknowledge-persistence
  (doseq [stage [:file-synced :renamed :directory-synced]]
    (let [dir (Files/createTempDirectory "netz-migration-fault-" (make-array FileAttribute 0))
          path (.resolve dir "checkpoint")
          key (byte-array 32)
          _ (.nextBytes (SecureRandom.) key)
          template (model/initial)]
      (try
        (let [saved (file/persist! path template key template)
              replica (first (keys (:attempts saved)))
              pending (:state (migration/step saved (model/receipt saved replica)))]
          (is (thrown? Exception
                       (binding [file/*after-stage*
                                 (fn [s] (when (= stage s)
                                           (throw (ex-info "Injected failure" {:stage s}))))]
                         (file/persist! path template key pending))))
          (is (not (migration/ready? pending)))
          (let [reopened (file/load! path template key 0)]
            (is (= (if (= stage :file-synced) 0 1) (:revision reopened)))
            (is (empty? (model/candidates reopened))))
          ;; Retrying the same proposed state is idempotent.
          (is (migration/ready? (file/persist! path template key pending))))
        (finally
          (Files/deleteIfExists path)
          (Files/deleteIfExists dir))))))
