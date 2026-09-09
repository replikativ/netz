(ns netz.replication-migration-konserve-test
  (:require [clojure.test :refer [deftest is]]
            [konserve.core :as k]
            [konserve.directory-sync :as directory-sync]
            [konserve.filestore :as fs]
            [konserve.impl.defaults :as defaults]
            [konserve.impl.storage-layout :as layout]
            [konserve.simulation.crash :as crash]
            [netz.replication.migration :as migration]
            [netz.replication.migration-konserve :as store]
            [netz.replication-migration-test :as model])
  (:import [java.nio.file Files]
           [java.io IOException]
           [java.nio.file.attribute FileAttribute]))

(deftest checkpoint-requires-explicit-provisioning-boundary
  (is (= :netz.replication.migration-konserve/missing-durable-ancestor
         (try (store/open! "unused" "unused" {}) nil
              (catch clojure.lang.ExceptionInfo e (:type (ex-data e)))))))

(deftest no-store-open-or-checkpoint-before-provisioning-completes
  (doseq [fail-at (range 1 6)]
    (let [root (Files/createTempDirectory "netz-checkpoint-provision-" (make-array FileAttribute 0))
          path (.resolve root "a/b")
          key ((ns-resolve 'konserve.encryptor 'generate-key))
          options {:durable-ancestor root}
          calls (atom 0) opens (atom 0)
          template (model/initial)]
      (try
        (with-redefs [directory-sync/sync-directory!
                      (fn [_] (when (= fail-at (swap! calls inc))
                                (throw (IOException. "provisioning barrier failed"))))
                      fs/connect-fs-store (fn [& _] (swap! opens inc))]
          (is (thrown-with-msg? IOException #"provisioning barrier failed"
                                (store/open! path key options))))
        (is (zero? @opens))
        (let [adapter (store/open! path key options)
              saved (store/persist! adapter template template)]
          (is (migration/ready? saved))
          (is (= saved (store/load! (store/open! path key options) template 0))))
        (finally (fs/delete-store (str root)))))))

(deftest encrypted-cbor-checkpoint-reopen
  (let [path (str (Files/createTempDirectory "netz-konserve-checkpoint-" (make-array FileAttribute 0)))
        key ((ns-resolve 'konserve.encryptor 'generate-key))
        template (model/initial)]
    (try
      (let [adapter (store/open! path key {:durable-ancestor path})]
        (is (thrown? Exception (store/load! adapter template 0)))
        (let [saved (store/persist! adapter template template)]
          (is (migration/ready? saved))
          (is (= saved (store/load! (store/open! path key {:durable-ancestor path}) template 0)))
          (is (thrown? Exception (store/load! adapter template 1)))
          (is (thrown? Exception
                       (store/load! adapter (assoc-in template [:binding :object] "wrong") 0)))
          (is (thrown? Exception
                       (store/load! (store/open! path (apply str (repeat 64 "0"))
                                                 {:durable-ancestor path}) template 0)))
          (is (empty? (model/candidates saved)))))
      (finally (fs/delete-store path)))))

(deftest bounded-authenticated-binary-read
  (let [path (str (Files/createTempDirectory "netz-checkpoint-corrupt-" (make-array FileAttribute 0)))
        key ((ns-resolve 'konserve.encryptor 'generate-key))
        template (model/initial)]
    (try
      (let [adapter (store/open! path key {:durable-ancestor path})
            backing (:store adapter)
            key :netz.replication.migration-konserve/checkpoint]
        (store/persist! adapter template template)
        (let [original (k/bget backing key
                               (fn [{:keys [input-stream]}] (.readAllBytes ^java.io.InputStream input-stream))
                               {:sync? true :raw? true :streaming? true})]
          (aset-byte original 40 (unchecked-byte (bit-xor 1 (aget original 40))))
          (k/bassoc backing key original {:sync? true :raw? true})
          (is (thrown? Exception (store/load! adapter template 0))))
        (doseq [bad [(byte-array 12) (byte-array (inc store/max-checkpoint-bytes))]]
          (k/bassoc backing key bad {:sync? true :raw? true})
          (is (thrown? Exception (store/load! adapter template 0)))))
      (finally (fs/delete-store path)))))

(deftest checkpoint-adapter-at-each-konserve-crash-boundary
  (doseq [point [:after-write-header :after-write-meta :after-write-value
                 :after-sync :after-atomic-move :after-sync-store]]
    (let [{:keys [backing state-atom crash-point-atom]} (crash/create-crash-aware-store)
          _ (layout/-create-store backing {:sync? true})
          config {:opts {:sync? true}
                  :config {:encoding {:serializer :BoringSerializer
                                      :encryptor {:type :aes-gcm
                                                  :key ((ns-resolve 'konserve.encryptor 'generate-key))
                                                  :require-authenticated? true}}}}
          adapter {:store (defaults/connect-default-store backing config)}
          template (model/initial)
          saved (store/persist! adapter template template)
          replica (first (keys (:attempts saved)))
          pending (:state (migration/step saved (model/receipt saved replica)))]
      (crash/set-crash-point! crash-point-atom point)
      (is (thrown? Exception (store/persist! adapter template pending)))
      (is (not (migration/ready? pending)))
      (crash/simulate-crash! state-atom)
      (crash/clear-crash-point! crash-point-atom)
      (let [reopened {:store (defaults/connect-default-store backing config)}
            recovered (store/load! reopened template 0)]
        (is (contains? #{0 1} (:revision recovered)))
        (is (empty? (model/candidates recovered)))
        (is (migration/ready? (store/persist! reopened template pending)))))))
